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

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "share/ob_thread_pool.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/blocksstable/ob_local_file_system.h"

namespace oceanbase {
using namespace blocksstable;
using namespace storage;

namespace unittest {

class TestStorageFile : public TestDataFilePrepare {
public:
  TestStorageFile();
  virtual ~TestStorageFile();
  virtual void SetUp();
  virtual void TearDown();

private:
  ObStorageFile* pg_file_;
};

TestStorageFile::TestStorageFile() : TestDataFilePrepare("TestStorageFile", 64 * 1024, 10240), pg_file_(NULL)
{}

TestStorageFile::~TestStorageFile()
{}

void TestStorageFile::SetUp()
{
  int ret = OB_SUCCESS;
  ret = ObTenantManager::get_instance().init(100000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().add_tenant(OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  TestDataFilePrepare::SetUp();
}

void TestStorageFile::TearDown()
{
  TestDataFilePrepare::TearDown();
  ObTenantManager::get_instance().destroy();
}

class TestStorageFileRefCnt : public share::ObThreadPool {
public:
  enum OptType { INC, DEC, MAX };
  TestStorageFileRefCnt();
  virtual ~TestStorageFileRefCnt() = default;
  int init(const int64_t thread_cnt, ObStorageFile* pg_file, OptType type);
  virtual void run1();

private:
  int do_work(const MacroBlockId& macro_id);
  ObStorageFile* pg_file_;
  OptType type_;
  int64_t thread_cnt_;
};

TestStorageFileRefCnt::TestStorageFileRefCnt() : pg_file_(NULL), type_(OptType::MAX), thread_cnt_(0)
{}

int TestStorageFileRefCnt::init(const int64_t thread_cnt, ObStorageFile* pg_file, OptType type)
{
  int ret = OB_SUCCESS;
  if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else {
    thread_cnt_ = thread_cnt;
    pg_file_ = pg_file;
    type_ = type;
    set_thread_count(static_cast<int32_t>(thread_cnt_));
  }
  return ret;
}

void TestStorageFileRefCnt::run1()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id(1);
  int count = 1000;
  while (count--) {
    int i = 0;
    if (OptType::INC == type_) {
      i = 0;
    } else {
      i = 1;
    }
    for (; i < 10; i++) {
      ret = do_work(macro_id);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    macro_id.block_index_++;
    // STORAGE_LOG(WARN, "jinzhu debug", K(macro_id));
  }
  // STORAGE_LOG(WARN, "jinzhu debug", K(count));
}

int TestStorageFileRefCnt::do_work(const MacroBlockId& macro_id)
{
  int ret = OB_SUCCESS;
  switch (type_) {
    case OptType::INC:
      ret = pg_file_->inc_ref(macro_id);
      break;
    case OptType::DEC:
      ret = pg_file_->dec_ref(macro_id);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "type_ is an invalid argument");
  }
  return ret;
}

TEST_F(TestStorageFile, test_inc_and_dec_ref)
{
  int ret = OB_SUCCESS;
  ret = util_.get_file_system().alloc_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init_base(ObStorageFile::FileType::TMP_FILE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId macro_id(1);

  int count = 10;
  while (count--) {
    ret = pg_file_->inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 9;
  while (count--) {
    ret = pg_file_->dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(1, pg_file_->macro_block_info_.count());

  ObStorageFile::BlockInfo block_info;
  ret = pg_file_->macro_block_info_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);

  ret = util_.get_file_system().free_file(pg_file_);
}

TEST_F(TestStorageFile, test_multi_inc_and_dec_ref)
{
  int ret = OB_SUCCESS;
  ret = util_.get_file_system().alloc_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init_base(ObStorageFile::FileType::SERVER_ROOT, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  TestStorageFileRefCnt inc_ref;
  TestStorageFileRefCnt dec_ref;
  ret = inc_ref.init(32, pg_file_, TestStorageFileRefCnt::OptType::INC);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = dec_ref.init(32, pg_file_, TestStorageFileRefCnt::OptType::DEC);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_ref.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  inc_ref.wait();
  ret = dec_ref.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  dec_ref.wait();

  ret = util_.get_file_system().free_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestStorageFile, test_overmuch_dec_ref)
{
  int ret = OB_SUCCESS;
  ret = util_.get_file_system().alloc_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init_base(ObStorageFile::FileType::TMP_FILE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId macro_id(1);

  int count = 10;
  while (count--) {
    ret = pg_file_->inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 11;
  while (OB_SUCC(ret) && count--) {
    ret = pg_file_->dec_ref(macro_id);
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(0, pg_file_->macro_block_info_.count());

  ret = util_.get_file_system().free_file(pg_file_);
}

TEST_F(TestStorageFile, test_1_0_1)
{
  int ret = OB_SUCCESS;
  ret = util_.get_file_system().alloc_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init_base(ObStorageFile::FileType::TMP_FILE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId macro_id(1);

  int count = 10;
  while (count--) {
    ret = pg_file_->inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 10;
  while (count--) {
    ret = pg_file_->dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(0, pg_file_->macro_block_info_.count());

  count = 10;
  while (count--) {
    ret = pg_file_->inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 9;
  while (count--) {
    ret = pg_file_->dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(1, pg_file_->macro_block_info_.count());

  ObStorageFile::BlockInfo block_info;
  ret = pg_file_->macro_block_info_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);

  ret = util_.get_file_system().free_file(pg_file_);
}

/*TEST_F(TestStorageFile, test_not_enough_mem)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1001;

  ret = ObTenantManager::get_instance().add_tenant(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id, 2 * 1024L * 1024L, 4 * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObConcurrentFIFOAllocator allocator;
  ret = allocator.init(1L * 1024L * 1024L, ObModIds::TEST, tenant_id, 1024L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = util_.get_file_system().alloc_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init_base(ObStorageFile::FileType::SERVER_ROOT, tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  CHUNK_MGR.set_limit(4 * 1024L * 1024L);

  void *buf;
  int i = 0;
  while (OB_NOT_NULL(buf = allocator.alloc(1 * 1024L))) {
    STORAGE_LOG(WARN, "tenant allocator", K(i++));
  }

  while (OB_NOT_NULL(buf = ob_malloc(1 * 1024L, pg_file_->macro_block_info_.memattr_))) {
    STORAGE_LOG(WARN, "ob_malloc", K(i++));
  }

  MacroBlockId macro_id(1);

  int64_t block_num = 1000000;
  i = 0;
  macro_id.write_seq_ = 0;
  while (OB_SUCC(ret) && i++ < block_num) {
    int count = 2;
    while (OB_SUCC(ret) && count--) {
      ret = pg_file_->inc_ref(macro_id);
    }
    count = 1;
    while (OB_SUCC(ret) && count--) {
      ret = pg_file_->dec_ref(macro_id);
    }
    if (macro_id.block_index_ < 10220) {
      macro_id.block_index_++;
    } else {
      macro_id.block_index_ = 1;
      macro_id.write_seq_++;
    }
    STORAGE_LOG(WARN, "jinzhu debug", K(ret), K(i));
  }
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);

  ObStorageFile::BlockInfo block_info;
  macro_id.set_real_block_id(1);
  ret = pg_file_->macro_block_info_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);

  ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id);

  ret = util_.get_file_system().free_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
}*/

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_ref_cnt.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ref_cnt.log", true, true);
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
