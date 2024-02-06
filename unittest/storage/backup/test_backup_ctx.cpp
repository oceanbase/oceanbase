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
#include "storage/backup/ob_backup_iterator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "test_backup.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class TestBackupCtx : public TestDataFilePrepare {
public:
  TestBackupCtx();
  virtual ~TestBackupCtx();
  virtual void SetUp();
  virtual void TearDown();

private:
  void inner_init_();
  int prepare_macro_block_list_(const int64_t macro_count, common::ObIArray<blocksstable::ObLogicMacroBlockId> &list);
  int prepare_meta_list_(const int64_t meta_count, common::ObIArray<common::ObTabletID> &list);
  void clean_env_();
  void build_backup_file_header_(ObBackupFileHeader &file_header);
  int do_backup_ctx_(const int64_t macro_count, const int64_t meta_count);
  void using_meta_iterator_(const common::ObIArray<ObBackupMetaIndex> &index_list);
  void using_macro_iterator_(const common::ObIArray<ObBackupMacroRangeIndex> &index_list);
  void convert_(const common::ObIArray<ObBackupMacroBlockIndex> &input_list,
      common::ObIArray<ObBackupMacroRangeIndex> &output_list);
  bool cmp_macro_index_(const common::ObIArray<ObBackupMacroRangeIndex> &lhs_list,
      const common::ObIArray<ObBackupMacroRangeIndex> &rhs_list);

protected:
  ObTenantBase tenant_base_;
  ObBackupJobDesc job_desc_;
  ObBackupDest backup_dest_;
  int64_t task_id_;
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
  ObLSBackupDataParam param_;
  common::ObInOutBandwidthThrottle throttle_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(TestBackupCtx);
};

TestBackupCtx::TestBackupCtx()
    : TestDataFilePrepare(&getter, "TestBackupTmpFile"),
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
      test_dir_(""),
      test_dir_uri_("")
{}

TestBackupCtx::~TestBackupCtx()
{}

void TestBackupCtx::SetUp()
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
  ret = ObTmpFileManager::get_instance().init();
  EXPECT_EQ(OB_SUCCESS, ret);
  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  inner_init_();
}

void TestBackupCtx::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

void TestBackupCtx::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_ctx", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, backup_dest_.get_storage_info()));
  task_id_ = 1;
  job_desc_.job_id_ = 1;
  job_desc_.task_id_ = 1;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_data_type_.set_major_data_backup();
  incarnation_ = 1;
  tenant_id_ = 1002;
  ls_id_ = ObLSID(1001);
  turn_id_ = 1;
  retry_id_ = 0;
  file_id_ = 0;
  param_.job_desc_ = job_desc_;
  param_.backup_stage_ = LOG_STREAM_BACKUP_MAJOR;
  param_.backup_dest_.deep_copy(backup_dest_);
  param_.tenant_id_ = tenant_id_;
  param_.backup_set_desc_ = backup_set_desc_;
  param_.ls_id_ = ls_id_;
  param_.backup_data_type_ = backup_data_type_;
  param_.turn_id_ = turn_id_;
  param_.retry_id_ = retry_id_;
  ret = throttle_.init(10);
  EXPECT_EQ(OB_SUCCESS, ret);
}

int TestBackupCtx::prepare_macro_block_list_(
    const int64_t macro_count, common::ObIArray<blocksstable::ObLogicMacroBlockId> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= macro_count; ++i) {
    const blocksstable::ObLogicMacroBlockId logic_id(1, 1, i);
    ret = list.push_back(logic_id);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  return ret;
}

int TestBackupCtx::prepare_meta_list_(const int64_t meta_count, common::ObIArray<common::ObTabletID> &list)
{
  int ret = OB_SUCCESS;
  list.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= meta_count; ++i) {
    const common::ObTabletID tablet_id(i);
    ret = list.push_back(tablet_id);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  return ret;
}

void TestBackupCtx::clean_env_()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

void TestBackupCtx::build_backup_file_header_(ObBackupFileHeader &file_header)
{
  file_header.magic_ = BACKUP_DATA_FILE_MAGIC;
  file_header.version_ = BACKUP_DATA_VERSION_V1;
  file_header.file_type_ = BACKUP_DATA_FILE;
  file_header.reserved_ = 0;
}

int TestBackupCtx::do_backup_ctx_(const int64_t macro_count, const int64_t meta_count)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObArenaAllocator allocator;
  ObBufferReader buffer_reader;
  ObBackupDataCtx backup_data_ctx;
  backup_data_ctx.open(param_, backup_data_type_, 0, throttle_);
  ObArray<ObLogicMacroBlockId> macro_list;
  ObArray<ObBackupMacroBlockIndex> macro_index_list;
  ObArray<ObTabletID> meta_list;
  ObArray<ObBackupMetaIndex> meta_index_list;
  prepare_macro_block_list_(macro_count, macro_list);
  prepare_meta_list_(meta_count, meta_list);
  ObBackupFileHeader file_header;
  build_backup_file_header_(file_header);
  backup_data_ctx.write_backup_file_header(file_header);
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_list.count(); ++i) {
    ObBackupMacroBlockIndex macro_index;
    const blocksstable::ObLogicMacroBlockId &logic_id = macro_list.at(i);
    ret = make_random_buffer(allocator, buffer_reader);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = backup_data_ctx.write_macro_block_data(buffer_reader, logic_id, macro_index);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = macro_index_list.push_back(macro_index);
    EXPECT_EQ(OB_SUCCESS, ret);
    allocator.reuse();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_list.count(); ++i) {
    ObBackupMetaIndex meta_index;
    const common::ObTabletID &tablet_id = meta_list.at(i);
    const ObBackupMetaType meta_type = BACKUP_TABLET_META;
    ret = make_random_buffer(allocator, buffer_reader);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = backup_data_ctx.write_meta_data(buffer_reader, tablet_id, meta_type, meta_index);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = meta_index_list.push_back(meta_index);
    EXPECT_EQ(OB_SUCCESS, ret);
    allocator.reuse();
  }
  ret = backup_data_ctx.close();
  EXPECT_EQ(OB_SUCCESS, ret);
  using_meta_iterator_(meta_index_list);
  ObArray<ObBackupMacroRangeIndex> macro_range_list;
  convert_(macro_index_list, macro_range_list);
  using_macro_iterator_(macro_range_list);
  return ret;
}

void TestBackupCtx::using_meta_iterator_(const common::ObIArray<ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndexIterator iter;
  ret = iter.init(task_id_,
      backup_dest_,
      tenant_id_,
      backup_set_desc_,
      ls_id_,
      backup_data_type_,
      turn_id_,
      retry_id_,
      false /*is_sec_meta*/,
      false /*need_read_inner_table*/);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObBackupMetaIndex meta_index;
  ObArray<ObBackupMetaIndex> read_index_list;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_cur_index(meta_index))) {
      LOG_WARN("failed to get cur index", K(ret));
    } else if (OB_FAIL(read_index_list.push_back(meta_index))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (OB_FAIL(iter.next())) {
      LOG_WARN("failed to next", K(ret));
    }
  }
  bool is_equal = cmp(index_list, read_index_list);
  EXPECT_TRUE(is_equal);
  const int64_t index_count = index_list.count();
  const common::ObArray<ObBackupMetaIndex> &cur_index_list = iter.cur_index_list_;
  const common::ObArray<ObBackupIndexBlockDesc> &block_desc_list = iter.block_desc_list_;
  for (int64_t i = 0; i < block_desc_list.count(); ++i) {
    const ObBackupIndexBlockDesc &block_desc = block_desc_list.at(i);
    if (0 == i) {
      EXPECT_EQ(0, block_desc.first_index_);
    }
    if (i == block_desc_list.count() - 1) {
      EXPECT_EQ(block_desc.last_index_, index_count - 1);
    } else {
      const ObBackupIndexBlockDesc &next_block_desc = block_desc_list.at(i + 1);
      EXPECT_EQ(block_desc.last_index_ + 1, next_block_desc.first_index_);
    }
  }
}

void TestBackupCtx::using_macro_iterator_(const common::ObIArray<ObBackupMacroRangeIndex> &index_list)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockIndexIterator iter;
  ret = iter.init(task_id_,
      backup_dest_,
      tenant_id_,
      backup_set_desc_,
      ls_id_,
      backup_data_type_,
      turn_id_,
      retry_id_,
      false /*need_read_inner_table*/);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObBackupMacroRangeIndex macro_index;
  ObArray<ObBackupMacroRangeIndex> read_index_list;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.get_cur_index(macro_index))) {
      LOG_WARN("failed to get cur index", K(ret));
    } else if (OB_FAIL(read_index_list.push_back(macro_index))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(iter.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  bool is_equal = cmp_macro_index_(index_list, read_index_list);
  EXPECT_TRUE(is_equal);
  const int64_t index_count = index_list.count();
  const common::ObArray<ObBackupMacroBlockIndex> &cur_index_list = iter.cur_index_list_;
  const common::ObArray<ObBackupIndexBlockDesc> &block_desc_list = iter.block_desc_list_;
  for (int64_t i = 0; i < block_desc_list.count(); ++i) {
    const ObBackupIndexBlockDesc &block_desc = block_desc_list.at(i);
    if (0 == i) {
      EXPECT_EQ(0, block_desc.first_index_);
    }
    if (i == block_desc_list.count() - 1) {
      EXPECT_EQ(block_desc.last_index_, index_count - 1);
    } else {
      const ObBackupIndexBlockDesc &next_block_desc = block_desc_list.at(i + 1);
      EXPECT_EQ(block_desc.last_index_ + 1, next_block_desc.first_index_);
    }
  }
}

void TestBackupCtx::convert_(
    const common::ObIArray<ObBackupMacroBlockIndex> &input_list, common::ObIArray<ObBackupMacroRangeIndex> &output_list)
{
  int ret = OB_SUCCESS;
  output_list.reset();
  for (int64_t i = 0; i < input_list.count(); ++i) {
    const ObBackupMacroBlockIndex &input = input_list.at(i);
    ObBackupMacroRangeIndex output;
    output.start_key_ = input.logic_id_;
    output.end_key_ = input.logic_id_;
    output.backup_set_id_ = input.backup_set_id_;
    output.ls_id_ = input.ls_id_;
    output.turn_id_ = input.turn_id_;
    output.retry_id_ = input.retry_id_;
    output.file_id_ = input.file_id_;
    ret = output_list.push_back(output);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

bool TestBackupCtx::cmp_macro_index_(const common::ObIArray<ObBackupMacroRangeIndex> &lhs_list,
    const common::ObIArray<ObBackupMacroRangeIndex> &rhs_list)
{
  bool bret = true;
  if (lhs_list.count() != rhs_list.count()) {
    bret = false;
    LOG_WARN("count not match", K(lhs_list.count()), K(rhs_list.count()));
  } else {
    for (int64_t i = 0; i < lhs_list.count(); ++i) {
      const ObBackupMacroRangeIndex &lhs = lhs_list.at(i);
      const ObBackupMacroRangeIndex &rhs = rhs_list.at(i);
      if (lhs.end_key_ == rhs.end_key_ && lhs.backup_set_id_ == rhs.backup_set_id_ && lhs.ls_id_ == rhs.ls_id_ &&
          lhs.turn_id_ == rhs.turn_id_ && lhs.retry_id_ == rhs.retry_id_ && lhs.file_id_ == rhs.file_id_) {
        bret = true;
      } else {
        bret = false;
        LOG_WARN("value do not match", K(lhs), K(rhs));
        break;
      }
    }
  }
  return bret;
}

static const int64_t ARRAY_SIZE = 4;
static const int64_t count_list[ARRAY_SIZE] = {2, 20, 200, 2000};

TEST_F(TestBackupCtx, only_meta_block)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAY_SIZE; ++i) {
    const int64_t count = count_list[i];
    EXPECT_EQ(OB_SUCCESS, do_backup_ctx_(0 /*macro_count*/, count /*meta_count*/));
  }
}

TEST_F(TestBackupCtx, only_macro_block)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAY_SIZE; ++i) {
    const int64_t count = count_list[i];
    EXPECT_EQ(OB_SUCCESS, do_backup_ctx_(count /*macro_count*/, 0 /*meta_count*/));
  }
}

TEST_F(TestBackupCtx, test_backup_ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAY_SIZE; ++i) {
    const int64_t count = count_list[i];
    const int64_t macro_count = random(0, count);
    const int64_t meta_count = count - macro_count;
    EXPECT_EQ(OB_SUCCESS, do_backup_ctx_(macro_count, meta_count));
  }
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_ctx.log*");
  OB_LOGGER.set_file_name("test_backup_ctx.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}