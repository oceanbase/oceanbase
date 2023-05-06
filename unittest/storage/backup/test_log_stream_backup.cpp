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
#define private public
#define protected public

#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_reader.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_index_cache.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/backup/ob_backup_task.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_handler.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

class TestLogStreamBackup : public TestDataFilePrepare {
public:
  TestLogStreamBackup();
  virtual ~TestLogStreamBackup();
  virtual void SetUp();
  virtual void TearDown();

protected:
  int prepare_random_buffer_(common::ObIAllocator &allocator, const int64_t size, ObBufferReader &buffer_reader);

protected:
  ObTenantDagScheduler *scheduler_;
  ObTenantBase tenant_base_;
  ObBackupJobDesc job_desc_;
  ObBackupDest backup_dest_;
  int64_t incarnation_;
  int64_t tenant_id_;
  ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  int64_t lower_mem_limit_;
  int64_t upper_mem_limit_;
  common::ObInOutBandwidthThrottle throttle_;
  ObBackupSetDesc full_backup_set_desc_;
  ObBackupSetDesc inc_backup_set_desc_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestLogStreamBackup);
};

TestLogStreamBackup::TestLogStreamBackup()
    : TestDataFilePrepare("TestLogStreamBackup", 2 * 1024 * 1024, 2048),
      scheduler_(NULL),
      tenant_base_(500),
      job_desc_(),
      backup_dest_(),
      incarnation_(),
      tenant_id_(OB_INVALID_ID),
      backup_set_desc_(),
      ls_id_(),
      backup_data_type_(),
      turn_id_(-1),
      retry_id_(-1),
      file_id_(-1),
      lower_mem_limit_(8 * 1024 * 1024),
      upper_mem_limit_(16 * 1024 * 1024),
      full_backup_set_desc_(),
      inc_backup_set_desc_(),
      test_dir_(""),
      test_dir_uri_("")
{}

TestLogStreamBackup::~TestLogStreamBackup()
{}

void TestLogStreamBackup::SetUp()
{
  int ret = OB_SUCCESS;
  ObUnitInfoGetter::ObTenantConfig unit_config;
  unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
  unit_config.tenant_id_ = 0;
  TenantUnits units;
  ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

  scheduler_ = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
  tenant_base_.set(scheduler_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ObBackupIoAdapter util;
  const ObString storage_info;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = lib::ACHUNK_SIZE;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_storage", get_current_dir_name());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("clean test_storage dir");
  // ASSERT_EQ(0, ::system("rm -fr test_storage"));
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));
  job_desc_.job_id_ = 1;
  job_desc_.task_id_ = 1;
  ret = backup_dest_.set(test_dir_uri_);
  ASSERT_EQ(OB_SUCCESS, ret);
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_set_desc_.backup_date_ = 20211231;
  backup_data_type_.set_major_data_backup();
  incarnation_ = 1;
  tenant_id_ = 1;
  ls_id_ = ObLSID(1);
  turn_id_ = 1;
  retry_id_ = 0;
  file_id_ = 0;
  full_backup_set_desc_.backup_set_id_ = 1;
  full_backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  full_backup_set_desc_.backup_date_ = 20211231;
  inc_backup_set_desc_.backup_set_id_ = 2;
  inc_backup_set_desc_.backup_type_.type_ = ObBackupType::INCREMENTAL_BACKUP;
  inc_backup_set_desc_.backup_date_ = 20220101;
  ret = throttle_.init(10);
  ASSERT_EQ(OB_SUCCESS, ret);
  TestDataFilePrepare::SetUp();
  ret = ObTenantManager::get_instance().init(100000);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().add_tenant(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id_, lower_mem_limit_, upper_mem_limit_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  CHUNK_MGR.set_limit(5L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLogStreamBackup::TearDown()
{
  scheduler_->destroy();
  tenant_base_.destroy();
  ObTenantEnv::set_tenant(nullptr);
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTenantManager::get_instance().destroy();
  TestDataFilePrepare::TearDown();
  LOG_INFO("clean test_storage dir");
  // ASSERT_EQ(0, ::system("rm -fr test_storage"));
}

int TestLogStreamBackup::prepare_random_buffer_(
    common::ObIAllocator &allocator, const int64_t size, ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      buf[i] = static_cast<char>(i % 256);
    }
    if (OB_SUCC(ret)) {
      buffer_reader.assign(buf, size, size);
    }
  }
  return ret;
}

void wait_scheduler()
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  while (!scheduler->is_empty()) {
    lib::this_routine::usleep(100000);
  }
}

TEST_F(TestLogStreamBackup, test_backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const char *root_path = "file:///obbackup";

  const char *expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full";
  ret = ObBackupPathUtil::get_backup_set_dir_path(root_path, tenant_id_, backup_set_desc_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/logstream_1";
  ret = ObBackupPathUtil::get_ls_backup_dir_path(root_path, tenant_id_, backup_set_desc_, ls_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/logstream_1/"
                "major_data_turn_1_retry_0";
  ret = ObBackupPathUtil::get_ls_backup_data_dir_path(
      root_path, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/logstream_1/"
                "major_data_turn_1_retry_0/macro_block_data.0";
  ret = ObBackupPathUtil::get_macro_block_backup_path(
      root_path, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, file_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/logstream_1/"
                "major_data_turn_1_retry_0/macro_range_index";
  ret = ObBackupPathUtil::get_ls_macro_range_index_backup_path(
      root_path, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/logstream_1/"
                "major_data_turn_1_retry_0/meta_index";
  ret = ObBackupPathUtil::get_ls_meta_index_backup_path(
      root_path, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/infos/ls_meta_info";
  ret = ObBackupPathUtil::get_ls_meta_info_backup_path(root_path, tenant_id_, backup_set_desc_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/infos/info_turn_1/"
                "tenant_major_data_macro_range_index";
  ret = ObBackupPathUtil::get_tenant_macro_range_index_backup_path(
      root_path, tenant_id_, backup_set_desc_, backup_data_type_, turn_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  expect_path = "file:///obbackup/tenant_1_incarnation_1/data/backup_set_1_full/infos/info_turn_1/"
                "tenant_major_data_meta_index";
  ret = ObBackupPathUtil::get_tenant_meta_index_backup_path(
      root_path, tenant_id_, backup_set_desc_, backup_data_type_, turn_id_, path);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));

  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("dump path", K(path), K(expect_path));
  ASSERT_EQ(0, path.get_obstr().compare(expect_path));
}

TEST_F(TestLogStreamBackup, test_backup_extern_info_mgr)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSetDesc> backup_set_desc_list;
  ret = backup_set_desc_list.push_back(full_backup_set_desc_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = backup_set_desc_list.push_back(inc_backup_set_desc_);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t index = 0; OB_SUCC(ret) && index < backup_set_desc_list.count(); ++index) {
    const ObBackupSetDesc &backup_set_desc = backup_set_desc_list.at(index);
    ObBackupDataStore extern_mgr;
    ret = extern_mgr.init(backup_dest_, tenant_id_, backup_set_desc);
    ASSERT_EQ(OB_SUCCESS, ret);
    static const int64_t total_ls_count = 3;
    static const int64_t fake_tablet_count = 1024;
    ObBackupDataTabletToLSDesc ls_desc;

    for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
      ObBackupDataTabletToLSInfo tablet_to_ls_info;
      const share::ObLSID ls_id(i);
      tablet_to_ls_info.ls_id_ = ls_id;
      for (int64_t j = 1 + i * fake_tablet_count; j <= (i + 1) * fake_tablet_count; ++j) {
        ObTabletID tablet_id(j);
        ret = tablet_to_ls_info.tablet_id_list_.push_back(tablet_id);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = ls_desc.tablet_to_ls_.push_back(tablet_to_ls_info);
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    ret = extern_mgr.write_tablet_to_ls_info(ls_desc, turn_id_);
    ASSERT_EQ(OB_SUCCESS, ret);

    for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
      const share::ObLSID ls_id(i);
      ObArray<ObTabletID> tablet_ids;
      ret = extern_mgr.read_tablet_to_ls_info(turn_id_, ls_id, tablet_ids);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(fake_tablet_count, tablet_ids.count());
      for (int64_t j = 1 + i * fake_tablet_count; j <= (i + 1) * fake_tablet_count; ++j) {
        ObTabletID tablet_id(j);
        ASSERT_EQ(tablet_id, tablet_ids.at(j - 1 - i * fake_tablet_count));
      }
    }
  }
}

TEST_F(TestLogStreamBackup, test_backup_struct)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_size = 4096;
  ObBufferReader buffer_reader;
  ret = prepare_random_buffer_(allocator, data_size, buffer_reader);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupCommonHeader common_header;
  common_header.data_type_ = 0;
  common_header.header_version_ = 0;
  common_header.data_version_ = 0;
  common_header.compressor_type_ = 0;
  common_header.header_length_ = sizeof(ObBackupCommonHeader);
  common_header.data_length_ = data_size;
  common_header.data_zlength_ = data_size;
  common_header.align_length_ = 0;
  ret = common_header.set_checksum(buffer_reader.data(), data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = common_header.check_header_checksum();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = common_header.check_data_checksum(buffer_reader.data(), data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogStreamBackup, test_backup_tmp_file)
{
  int ret = OB_SUCCESS;
  ObBackupIndexBufferNode buffer_node;
  const ObBackupBlockType block_type = BACKUP_BLOCK_MACRO_INDEX;
  const int64_t node_level = 1;
  ret = buffer_node.init(tenant_id_, block_type, node_level);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t loop_count = 1024;
  ObBackupMacroBlockIndex index;
  for (int64_t i = 1; OB_SUCC(ret) && i <= loop_count; ++i) {
    index.reset();
    index.logic_id_ = blocksstable::ObLogicMacroBlockId(1, 1, i);
    index.backup_set_id_ = backup_set_desc_.backup_set_id_;
    index.ls_id_ = ObLSID(i);
    index.turn_id_ = turn_id_;
    index.retry_id_ = retry_id_;
    index.file_id_ = file_id_;
    index.offset_ = 0;
    index.length_ = 4096;
    ret = buffer_node.put_backup_index(index);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 1; OB_SUCC(ret) && i <= loop_count; ++i) {
    ObBackupMacroBlockIndex new_index;
    ret = buffer_node.get_backup_index(new_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    blocksstable::ObLogicMacroBlockId logic_id(1, 1, i);
    ASSERT_EQ(logic_id, new_index.logic_id_);
  }
}

TEST_F(TestLogStreamBackup, test_sys_tablet_list)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id(1);
  common::ObArray<common::ObTabletID> tablet_id_list;
  ret = ObILSTabletIdReader::get_sys_tablet_list_(ls_id, tablet_id_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, tablet_id_list.count());
}

TEST_F(TestLogStreamBackup, test_backup_utils)
{
  int ret = OB_SUCCESS;
  ObBackupTabletProvider provider;
  ObLSBackupParam param;
  param.task_id_ = 1;
  param.backup_dest_ = backup_dest_;
  param.tenant_id_ = tenant_id_;
  param.backup_set_desc_ = backup_set_desc_;
  param.ls_id_ = ls_id_;
  param.turn_id_ = turn_id_;
  param.retry_id_ = retry_id_;
  ObLSBackupCtx ls_backup_ctx;
  ret = provider.init(param, backup_data_type_, ls_backup_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletID tablet_id(1);
  ObITable::TableKey table_key;
  const int64_t macro_id_count = 10 * 1024;
  ObArray<ObBackupMacroBlockId> tmp_array;
  for (int64_t i = macro_id_count; OB_SUCC(ret) && i > 0; --i) {
    blocksstable::ObLogicMacroBlockId logic_id(i, 1, tablet_id.id());
    MacroBlockId macro_block_id;
    ObBackupMacroBlockId macro_id;
    macro_id.logic_id_ = logic_id;
    macro_id.macro_block_id_ = macro_block_id;
    ret = tmp_array.push_back(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (tmp_array.count() >= 1024) {
      ret = provider.add_macro_block_id_item_list_(tablet_id, table_key, tmp_array);
      ASSERT_EQ(OB_SUCCESS, ret);
      tmp_array.reuse();
    }
  }
  ret = provider.external_sort_.do_sort(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArray<ObBackupProviderItem> provider_items;
  const int64_t batch_size = 1024;
  int64_t round = 0;
  while (OB_SUCC(ret)) {
    ++round;
    ret = provider.inner_get_batch_items_(batch_size, provider_items);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (provider_items.empty()) {
      LOG_INFO("provider items empty", K(round));
      break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < provider_items.count() - 1; ++i) {
      const ObBackupProviderItem &cur_item = provider_items.at(i);
      const ObBackupProviderItem &next_item = provider_items.at(i + 1);
      if (cur_item.get_logic_id() > next_item.get_logic_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("external sort not correct",
            K(cur_item.get_logic_id()),
            K(next_item.get_logic_id()),
            K(i),
            K(round),
            K(provider_items.count()));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
}

TEST_F(TestLogStreamBackup, test_backup_ctx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObBackupDataCtx backup_data_ctx;
  ObLSBackupDataParam param;
  param.job_desc_ = job_desc_;
  param.backup_dest_ = backup_dest_;
  param.tenant_id_ = tenant_id_;
  param.backup_set_desc_ = backup_set_desc_;
  param.ls_id_ = ls_id_;
  param.turn_id_ = turn_id_;
  param.retry_id_ = retry_id_;
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = backup_data_ctx.open(param, backup_data_type_, file_id_, throttle_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBufferReader buffer_reader;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 10; ++i) {
    blocksstable::blocksstable::ObLogicMacroBlockId logic_id(1, 1, i);
    ret = prepare_random_buffer_(allocator, i * 1024, buffer_reader);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = backup_data_ctx.write_macro_block_data(buffer_reader, logic_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("write macro block data", K(i), K(logic_id));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i <= 10; ++i) {
    common::ObTabletID tablet_id(i);
    const ObBackupMetaType meta_type = BACKUP_TABLET_META;
    ret = prepare_random_buffer_(allocator, i * 1024, buffer_reader);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = backup_data_ctx.write_meta_data(buffer_reader, tablet_id, meta_type);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("write meta data", K(i), K(tablet_id));
  }
  ret = backup_data_ctx.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogStreamBackup, test_backup_iterator)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockIndexIterator macro_index_iterator;
  ret = macro_index_iterator.init(
      backup_dest_, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupMacroRangeIndex range_index;
  while (OB_SUCC(ret) && !macro_index_iterator.is_iter_end()) {
    range_index.reset();
    ret = macro_index_iterator.get_cur_index(range_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = macro_index_iterator.next();
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get backup macro range index", K(range_index));
  }

  ObBackupMetaIndexIterator meta_index_iterator;
  ret = meta_index_iterator.init(
      backup_dest_, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupMetaIndex meta_index;
  while (OB_SUCC(ret) && !meta_index_iterator.is_iter_end()) {
    meta_index.reset();
    ret = meta_index_iterator.get_cur_index(meta_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = meta_index_iterator.next();
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get backup meta index", K(meta_index));
  }
}

TEST_F(TestLogStreamBackup, test_backup_index_merger)
{
  int ret = OB_SUCCESS;
  const ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_LOG_STREAM;
  ObBackupMacroBlockIndexMerger macro_index_merger;
  ret = macro_index_merger.init(backup_dest_,
      tenant_id_,
      backup_set_desc_,
      backup_data_type_,
      index_level,
      ls_id_,
      turn_id_,
      retry_id_,
      throttle_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = macro_index_merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBackupMetaIndexMerger meta_index_merger;
  ret = meta_index_merger.init(backup_dest_,
      tenant_id_,
      backup_set_desc_,
      backup_data_type_,
      index_level,
      ls_id_,
      turn_id_,
      retry_id_,
      throttle_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = meta_index_merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogStreamBackup, test_backup_index_cache)
{
  int ret = OB_SUCCESS;
  ObBackupIndexKVCache index_cache;
  const char *cache_name = "backup_index_kv_cache";
  const int64_t priority = 1;
  ret = index_cache.init(cache_name, priority);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupIndexCacheKey cache_key;
  ObBackupRestoreMode mode = BACKUP_MODE;
  const int64_t offset = 0;
  const int64_t length = 16 * 1024;
  ret = cache_key.set(
      mode, tenant_id_, incarnation_, backup_set_desc_.backup_set_id_, turn_id_, retry_id_, file_id_, offset, length);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t buf_len = 16 * 1024;
  char *buf = new char[buf_len];
  for (int64_t i = 0; i < buf_len; ++i) {
    buf[i] = static_cast<char>(i % 256);
  }
  ObBackupIndexCacheValue cache_value(buf, buf_len);
  ret = index_cache.put(cache_key, cache_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObKVCacheHandle cache_handle;
  const ObBackupIndexCacheValue *new_cache_value = NULL;
  ret = index_cache.get(cache_key, new_cache_value, cache_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  index_cache.destroy();
}

TEST_F(TestLogStreamBackup, test_backup_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupIndexKVCache kv_cache;
  const char *cache_name = "kv_cache";
  const int64_t priority = 1;
  ret = kv_cache.init(cache_name, priority);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupRestoreMode mode = RESTORE_MODE;
  ObBackupMacroBlockIndexStore macro_index_store;
  ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_LOG_STREAM;
  ret = macro_index_store.init(mode,
      index_level,
      backup_dest_,
      tenant_id_,
      backup_set_desc_,
      ls_id_,
      backup_data_type_,
      turn_id_,
      retry_id_,
      kv_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t loop_count = 10;
  ObBackupMacroBlockIndex macro_index;
  for (int64_t i = 1; OB_SUCC(ret) && i <= loop_count; ++i) {
    blocksstable::ObLogicMacroBlockId logic_id(1, 1, i);
    ret = macro_index_store.get_macro_block_index(logic_id, macro_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get backup macro block index", K(logic_id), K(macro_index));
    ASSERT_EQ(logic_id, macro_index.logic_id_);
  }

  ObBackupMetaIndexStore meta_index_store;
  ret = meta_index_store.init(mode,
      index_level,
      backup_dest_,
      tenant_id_,
      backup_set_desc_,
      ls_id_,
      backup_data_type_,
      turn_id_,
      retry_id_,
      kv_cache);
  ObBackupMetaIndex meta_index;
  for (int64_t i = 1; OB_SUCC(ret) && i <= loop_count; ++i) {
    ObTabletID tablet_id(i);
    ObBackupMetaType meta_type = BACKUP_TABLET_META;
    ret = meta_index_store.get_backup_meta_index(tablet_id, meta_type, meta_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get backup meta index", K(tablet_id), K(meta_index));
    ASSERT_EQ(tablet_id, meta_index.meta_key_.tablet_id_);
  }
}

TEST_F(TestLogStreamBackup, test_backup_ls_full_data)
{
  int ret = OB_SUCCESS;
  uint32_t time_slice = 20 * 1000;  // 5ms
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  ObAddr addr(1, 1);
  ret = ObSysTaskStatMgr::get_instance().set_self_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t static total_ls_count = 3;

  for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
    const share::ObLSID ls_id(i);
    ret = ObBackupHandler::schedule_backup_data_dag(
        job_desc_, backup_dest_, tenant_id_, full_backup_set_desc_, ls_id, turn_id_, retry_id_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  wait_scheduler();
  EXPECT_EQ(0, share::ObDagWarningHistoryManager::get_instance().size());
}

TEST_F(TestLogStreamBackup, test_build_tenant_level_full_index)
{
  int ret = OB_SUCCESS;
  uint32_t time_slice = 20 * 1000;  // 5ms
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  ObAddr addr(1, 1);
  ret = ObSysTaskStatMgr::get_instance().set_self_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBackupDataType minor_backup_type;
  minor_backup_type.set_minor_data_backup();
  ret = ObBackupHandler::schedule_build_tenant_level_index_dag(
      job_desc_, backup_dest_, tenant_id_, full_backup_set_desc_, turn_id_, minor_backup_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBackupDataType major_backup_type;
  major_backup_type.set_major_data_backup();
  ret = ObBackupHandler::schedule_build_tenant_level_index_dag(
      job_desc_, backup_dest_, tenant_id_, full_backup_set_desc_, turn_id_, major_backup_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  wait_scheduler();
  EXPECT_EQ(0, share::ObDagWarningHistoryManager::get_instance().size());
}

TEST_F(TestLogStreamBackup, test_backup_ls_inc_data)
{
  int ret = OB_SUCCESS;
  uint32_t time_slice = 20 * 1000;  // 5ms
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  ObAddr addr(1, 1);
  ret = ObSysTaskStatMgr::get_instance().set_self_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t static total_ls_count = 3;

  for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
    const share::ObLSID ls_id(i);
    ret = ObBackupHandler::schedule_backup_data_dag(
        job_desc_, backup_dest_, tenant_id_, inc_backup_set_desc_, ls_id, turn_id_, retry_id_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  wait_scheduler();
  EXPECT_EQ(0, share::ObDagWarningHistoryManager::get_instance().size());
}

TEST_F(TestLogStreamBackup, test_build_tenant_level_inc_index)
{
  int ret = OB_SUCCESS;
  uint32_t time_slice = 20 * 1000;  // 5ms
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  ObAddr addr(1, 1);
  ret = ObSysTaskStatMgr::get_instance().set_self_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBackupDataType minor_backup_type;
  minor_backup_type.set_minor_data_backup();
  ret = ObBackupHandler::schedule_build_tenant_level_index_dag(
      job_desc_, backup_dest_, tenant_id_, inc_backup_set_desc_, turn_id_, minor_backup_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObBackupDataType major_backup_type;
  major_backup_type.set_major_data_backup();
  ret = ObBackupHandler::schedule_build_tenant_level_index_dag(
      job_desc_, backup_dest_, tenant_id_, inc_backup_set_desc_, turn_id_, major_backup_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  wait_scheduler();
  EXPECT_EQ(0, share::ObDagWarningHistoryManager::get_instance().size());
}

TEST_F(TestLogStreamBackup, test_macro_range_index_iterator)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id(0);
  ObBackupMacroRangeIndexIterator full_iter;
  ret = full_iter.init(backup_dest_, tenant_id_, full_backup_set_desc_, ls_id, backup_data_type_, turn_id_, retry_id_);
  ASSERT_EQ(OB_SUCCESS, ret);

  while (OB_SUCC(ret) && !full_iter.is_iter_end()) {
    ObBackupMacroRangeIndex range_index;
    range_index.reset();
    ret = full_iter.get_cur_index(range_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = full_iter.next();
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get full backup macro range index", K(range_index));
  }

  ObBackupMacroRangeIndexIterator inc_iter;
  ret = inc_iter.init(backup_dest_, tenant_id_, inc_backup_set_desc_, ls_id, backup_data_type_, turn_id_, retry_id_);
  ASSERT_EQ(OB_SUCCESS, ret);

  while (OB_SUCC(ret) && !inc_iter.is_iter_end()) {
    ObBackupMacroRangeIndex range_index;
    range_index.reset();
    ret = inc_iter.get_cur_index(range_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = inc_iter.next();
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("get inc backup macro range index", K(range_index));
  }

  ObBackupIndexKVCache kv_cache;
  const char *cache_name = "kv_cache";
  const int64_t priority = 1;
  ret = kv_cache.init(cache_name, priority);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObBackupRestoreMode mode = RESTORE_MODE;
  ObBackupMacroBlockIndexStore macro_index_store;
  ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_TENANT;
  ret = macro_index_store.init(mode,
      index_level,
      backup_dest_,
      tenant_id_,
      backup_set_desc_,
      ls_id,
      backup_data_type_,
      turn_id_,
      retry_id_,
      kv_cache);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 1; OB_SUCC(ret) && i <= 1023; ++i) {
    blocksstable::ObLogicMacroBlockId logic_id(i, 1, 1);
    ObBackupMacroBlockIndex macro_index;
    ret = macro_index_store.get_macro_block_index(logic_id, macro_index);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 1; OB_SUCC(ret) && i <= 1023; ++i) {
    blocksstable::ObLogicMacroBlockId logic_id(i, 1, 2);
    ObBackupMacroBlockIndex macro_index;
    ret = macro_index_store.get_macro_block_index(logic_id, macro_index);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestLogStreamBackup, test_backup_complement_log)
{
  int ret = OB_SUCCESS;
  uint32_t time_slice = 20 * 1000;  // 5ms
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  ObAddr addr(1, 1);
  ret = ObSysTaskStatMgr::get_instance().set_self_addr(addr);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t static total_ls_count = 3;

  for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
    const share::ObLSID ls_id(i);
    ret = ObBackupHandler::schedule_backup_complement_log_dag(
        job_desc_, backup_dest_, tenant_id_, full_backup_set_desc_, ls_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  for (int64_t i = 1; OB_SUCC(ret) && i <= total_ls_count; ++i) {
    const share::ObLSID ls_id(i);
    ret = ObBackupHandler::schedule_backup_complement_log_dag(
        job_desc_, backup_dest_, tenant_id_, inc_backup_set_desc_, ls_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  wait_scheduler();
  EXPECT_EQ(0, share::ObDagWarningHistoryManager::get_instance().size());
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_log_stream_backup.log*");
  OB_LOGGER.set_file_name("test_log_stream_backup.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
