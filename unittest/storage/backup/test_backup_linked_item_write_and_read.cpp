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

#include "lib/utility/ob_test_util.h"

#include "storage/backup/test_backup.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/backup/ob_backup_linked_block_writer.h"
#include "storage/blocksstable/ob_data_file_prepare.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

static ObSimpleMemLimitGetter getter;

class TestBackupLinkedReaderWriter : public TestDataFilePrepare
{
public:
  TestBackupLinkedReaderWriter();
  virtual ~TestBackupLinkedReaderWriter();
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
  DISALLOW_COPY_AND_ASSIGN(TestBackupLinkedReaderWriter);
};

int prepare_backup_dest(ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  if (OB_FAIL(databuff_printf(test_dir_, sizeof(test_dir_),
      "%s/test_backup_linked_item_write_and_read_dir", get_current_dir_name()))) {
    LOG_WARN("failed to databuff_printf", K(ret));
  } else if (OB_FAIL(databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else if (OB_FAIL(backup_dest.set(test_dir_uri_))) {
    LOG_WARN("failed to set backup dest", K(ret));
  } else if (OB_FAIL(util.mkdir(test_dir_uri_, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to mkdir", K(ret));
  }
  return ret;
}

int prepare_backup_data_param(
    const ObBackupDest &backup_dest,
    ObLSBackupDataParam &param)
{
  int ret = OB_SUCCESS;
  if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(backup_dest));
  } else if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy", K(ret), K(backup_dest));
  } else {
    param.job_desc_.job_id_ = 1;
    param.job_desc_.task_id_ = 1;
    param.backup_stage_ = LOG_STREAM_BACKUP_MAJOR;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.dest_id_ = 1;
    param.backup_set_desc_.backup_set_id_ = 1;
    param.backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
    param.ls_id_ = ObLSID(1001);
    param.backup_data_type_.set_user_data_backup();
    param.turn_id_ = 1;
    param.retry_id_ = 0;
  }
  return ret;
}

int prepare_backup_set_dest(const ObLSBackupDataParam &param, ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(
      param.backup_dest_, param.backup_set_desc_, backup_set_dest))) {
    LOG_WARN("failed to set backup set dest", K(ret), K(param));
  }
  return ret;
}

int prepare_file_write_ctx(
    const ObLSBackupDataParam &param,
    const ObBackupDest &backup_set_dest,
    const int64_t file_id,
    common::ObIOFd &io_fd,
    ObInOutBandwidthThrottle &bandwidth_throttle,
    common::ObIODevice *&dev_handle,
    ObBackupFileWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter util;
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  share::ObBackupPath backup_path;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  const int64_t data_file_size = share::DEFAULT_BACKUP_DATA_FILE_SIZE;

  if (!param.is_valid() || !backup_set_dest.is_valid() || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_set_dest), K(file_id));;
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest,
      param.ls_id_, backup_data_type, param.turn_id_, param.retry_id_, file_id, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(param));
  } else if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), backup_set_dest.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(backup_path));
  } else if (OB_FAIL(util.open_with_access_type(dev_handle, io_fd,
      backup_set_dest.get_storage_info(), backup_path.get_obstr(), access_type,
      ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to open with access type", K(ret));
  } else if (OB_FAIL(write_ctx.open(data_file_size, io_fd, *dev_handle, bandwidth_throttle))) {
    LOG_WARN("failed to open write ctx", K(ret));
  }
  return ret;
}

int prepare_one_item(const int64_t idx, const int64_t file_id, const ObLSBackupDataParam &param, ObBackupLinkedItem &item)
{
  int ret = OB_SUCCESS;
  item.macro_id_.write_seq_ = idx;
  item.macro_id_.id_mode_ = static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL);
  item.macro_id_.version_ = MacroBlockId::MACRO_BLOCK_ID_VERSION_V2;
  item.macro_id_.block_index_ = idx;
  item.macro_id_.third_id_ = 0;
  item.macro_id_.fourth_id_ = 0;

  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  item.backup_id_.ls_id_ = param.ls_id_.id();
  item.backup_id_.data_type_ = backup_data_type.type_;
  item.backup_id_.turn_id_ = param.turn_id_;
  item.backup_id_.retry_id_ = param.retry_id_;
  item.backup_id_.file_id_ = file_id;
  item.backup_id_.backup_set_id_ = param.backup_set_desc_.backup_set_id_;
  item.backup_id_.offset_ = idx * OB_DEFAULT_MACRO_BLOCK_SIZE / 4096;
  item.backup_id_.length_ = OB_DEFAULT_MACRO_BLOCK_SIZE / 4096;
  item.backup_id_.block_type_ = ObBackupDeviceMacroBlockId::DATA_BLOCK;
  item.backup_id_.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
  item.backup_id_.version_ = ObBackupDeviceMacroBlockId::BACKUP_MACRO_BLOCK_ID_VERSION;
  return ret;
}

int prepare_write_item_list(const int64_t count, const int64_t file_id, const ObLSBackupDataParam &param,
    common::ObIArray<ObBackupLinkedItem> &item_list)
{
  int ret = OB_SUCCESS;
  item_list.reset();
  ObBackupLinkedItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(prepare_one_item(i, file_id, param, item))) {
      LOG_WARN("failed to prepare one item", K(ret), K(i));
    } else if (!item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid arg", K(ret), K(item));
    } else if (OB_FAIL(item_list.push_back(item))) {
      LOG_WARN("failed to push back", K(ret), K(item));
    }
  }
  return ret;
}

int write_item_to_writer(const common::ObIArray<ObBackupLinkedItem> &item_list, ObBackupLinkedBlockItemWriter &writer)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(item_list, idx, cnt, OB_SUCC(ret)) {
    const ObBackupLinkedItem &item = item_list.at(idx);
    if (OB_FAIL(writer.write(item))) {
      LOG_WARN("failed to write item", K(ret), K(item));
    }
  }
  return ret;
}

int read_item_from_reader(ObBackupLinkedBlockItemReader &reader, common::ObIArray<ObBackupLinkedItem> &item_list)
{
  int ret = OB_SUCCESS;
  ObBackupLinkedItem link_item;
  while (OB_SUCC(ret)) {
    link_item.reset();
    if (OB_FAIL(reader.get_next_item(link_item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next item", K(ret));
      }
    } else if (OB_FAIL(item_list.push_back(link_item))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

struct ObBackupLinkItemCompare
{
  bool operator()(const ObBackupLinkedItem &lhs, const ObBackupLinkedItem &rhs)
  {
    return lhs.macro_id_ < rhs.macro_id_;
  }
};

struct ObBackupPhysicalIDCompare
{
  bool operator()(const ObBackupDeviceMacroBlockId &lhs, const ObBackupDeviceMacroBlockId &rhs)
  {
    return lhs.offset_ < rhs.offset_;
  }
};

int sort_and_compare_item_list(
    common::ObArray<ObBackupLinkedItem> &item_list1,
    common::ObArray<ObBackupLinkedItem> &item_list2)
{
  int ret = OB_SUCCESS;
  ObBackupLinkItemCompare cmp;
  if (item_list1.count() != item_list2.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item list count not same", "count1", item_list1.count(), "count2", item_list2.count());
  } else {
    std::sort(item_list1.begin(), item_list1.end(), cmp);
    std::sort(item_list2.begin(), item_list2.end(), cmp);
    ARRAY_FOREACH_X(item_list1, idx, cnt, OB_SUCC(ret)) {
      const ObBackupLinkedItem &item1 = item_list1.at(idx);
      const ObBackupLinkedItem &item2 = item_list2.at(idx);
      if (item1 != item2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item not same", K(ret), K(idx), K(cnt), K(item1), K(item2));
      }
    }
  }
  return ret;
}

int sort_and_compare_block_list(
    common::ObArray<ObBackupDeviceMacroBlockId> &block_list1,
    common::ObArray<ObBackupDeviceMacroBlockId> &block_list2)
{
  int ret = OB_SUCCESS;
  ObBackupPhysicalIDCompare cmp;
  if (block_list1.count() != block_list2.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item list count not same", "count1", block_list1.count(), "count2", block_list2.count());
  } else {
    std::sort(block_list1.begin(), block_list1.end(), cmp);
    std::sort(block_list2.begin(), block_list2.end(), cmp);
    ARRAY_FOREACH_X(block_list1, idx, cnt, OB_SUCC(ret)) {
      const ObBackupDeviceMacroBlockId &id1 = block_list1.at(idx);
      const ObBackupDeviceMacroBlockId &id2 = block_list2.at(idx);
      if (id1 != id2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("item not same", K(ret), K(idx), K(cnt), K(id1), K(id2));
      }
    }
  }
  return ret;
}

TestBackupLinkedReaderWriter::TestBackupLinkedReaderWriter()
  : TestDataFilePrepare(&getter, "TestBackupIndexMerger", OB_DEFAULT_MACRO_BLOCK_SIZE, 800),
    tenant_base_(500)
{}

TestBackupLinkedReaderWriter::~TestBackupLinkedReaderWriter()
{}

void TestBackupLinkedReaderWriter::SetUp()
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

void TestBackupLinkedReaderWriter::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestBackupLinkedReaderWriter::clean_env_()
{
  char test_dir_[OB_MAX_URI_LENGTH];
  databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_linked_item_write_and_read_dir", get_current_dir_name());
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

int write_and_read_items(const int64_t item_count, const int64_t expected_block_count)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObLSBackupDataParam param;
  ObBackupDest backup_set_dest;
  common::ObTabletID tablet_id(200001);
  ObITable::TableKey table_key;
  make_random_table_key(table_key);
  const int64_t file_id = 0;
  ObIOFd io_fd;
  ObIODevice *device_handle = NULL;
  ObBackupFileWriteCtx write_ctx;
  ObArray<ObBackupLinkedItem> write_item_list;
  ObBackupLinkedBlockItemWriter writer;
  int64_t file_offset = 0;
  ObBackupDeviceMacroBlockId entry_block_id;
  int64_t total_block_count = 0;
  ObArray<ObBackupLinkedItem> read_item_list;
  ObBackupLinkedBlockItemReader reader;
  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "BACKUP");
  ObStorageIdMod id_mod;
  id_mod.storage_id_ = 1;
  id_mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  ObArray<ObBackupDeviceMacroBlockId> written_block_list;
  ObArray<ObBackupDeviceMacroBlockId> read_block_list;
  ObInOutBandwidthThrottle bandwidth_throttle;

  if (OB_FAIL(bandwidth_throttle.init(1024 * 1024 * 60)))  {
    LOG_WARN("failed to init bandwidth_throttle", K(ret), K(param));
  } else if (OB_FAIL(prepare_backup_dest(backup_dest))) {
    LOG_WARN("failed to prepare backup dest", K(ret));
  } else if (OB_FAIL(prepare_backup_data_param(backup_dest, param))) {
    LOG_WARN("failed to prepare backup data param", K(ret), K(backup_dest));
  } else if (OB_FAIL(prepare_backup_set_dest(param, backup_set_dest))) {
    LOG_WARN("failed to prepare backup set dest", K(ret), K(param));
  } else if (OB_FAIL(prepare_file_write_ctx(param, backup_set_dest, file_id, io_fd, bandwidth_throttle, device_handle, write_ctx))) {
    LOG_WARN("failed to prepare file write ctx", K(ret), K(param), K(backup_set_dest));
  } else if (OB_FAIL(prepare_write_item_list(item_count, file_id, param, write_item_list))) {
    LOG_WARN("failed to prepare write item list", K(ret));
  } else if (OB_FAIL(writer.init(param, tablet_id, table_key, file_id, write_ctx, file_offset))) {
    LOG_WARN("failed to init writer", K(ret));
  } else if (OB_FAIL(write_item_to_writer(write_item_list, writer))) {
    LOG_WARN("failed to write item to writer", K(ret));
  } else if (OB_FAIL(writer.close())) {
    LOG_WARN("failed to close writer", K(ret), K(entry_block_id));
  } else if (OB_FAIL(write_ctx.flush_buffer_(true/*is_last_part*/))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else if (OB_FAIL(writer.get_root_block_id(entry_block_id, total_block_count))) {
    LOG_WARN("failed to get root block id", K(ret));
  } else if (OB_FALSE_IT(written_block_list = writer.get_written_block_list())) {
  } else if (OB_FAIL(device_handle->complete(io_fd))) {
    LOG_WARN("failed to complete device handle", K(ret));
  } else if (OB_FAIL(reader.init(backup_set_dest, entry_block_id,
      total_block_count, tablet_id, table_key, mem_attr, id_mod))) {
    LOG_WARN("failed to init reader", K(ret));
  } else if (OB_FAIL(read_item_from_reader(reader, read_item_list))) {
    LOG_WARN("failed to read item from reader", K(ret));
  } else if (OB_FALSE_IT(read_block_list = reader.get_read_block_list())) {
  } else if (OB_FAIL(sort_and_compare_item_list(write_item_list, read_item_list))) {
    LOG_WARN("failed to sort and compare item list", K(ret));
  } else if (OB_FAIL(sort_and_compare_block_list(written_block_list, read_block_list))) {
    LOG_WARN("failed to sort and compare block list", K(ret));
  } else if (expected_block_count != written_block_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected block count not same", K(ret), K(expected_block_count), K(written_block_list.count()));
  }
  return ret;
}

TEST_F(TestBackupLinkedReaderWriter, write_and_read_one_block)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t item_count = 1000;
  const int64_t expected_block_count = 1;
  OK(write_and_read_items(item_count, expected_block_count));
}

TEST_F(TestBackupLinkedReaderWriter, write_and_read_two_block)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t item_count = 60 * 1000;
  const int64_t expected_block_count = 2;
  OK(write_and_read_items(item_count, expected_block_count));
}

TEST_F(TestBackupLinkedReaderWriter, write_and_read_three_block)
{
  int ret = OB_SUCCESS;
  clean_env_();
  const int64_t item_count = 90 * 1000;
  const int64_t expected_block_count = 3;
  OK(write_and_read_items(item_count, expected_block_count));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_linked_item_write_and_read.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_linked_item_write_and_read.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
