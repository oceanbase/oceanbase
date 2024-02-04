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

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/ob_thread_pool.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
using namespace common;
static ObSimpleMemLimitGetter getter;

namespace unittest
{

class TestRefCnt : public TestDataFilePrepare
{
public:
  TestRefCnt();
  virtual ~TestRefCnt();
  virtual void SetUp();
  virtual void TearDown();
};

TestRefCnt::TestRefCnt()
  : TestDataFilePrepare(&getter, "TestRefCnt", 64 * 1024, 10240)
{
}

TestRefCnt::~TestRefCnt()
{
}

void TestRefCnt::SetUp()
{
  int ret = OB_SUCCESS;
  ret = getter.add_tenant(OB_SERVER_TENANT_ID,
                          2 * 1024L * 1024L, 4 * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  TestDataFilePrepare::SetUp();
}

void TestRefCnt::TearDown()
{
  TestDataFilePrepare::TearDown();
}

class TestStorageFileRefCnt : public share::ObThreadPool
{
public:
  enum OptType {INC, DEC, MAX};
  TestStorageFileRefCnt();
  virtual ~TestStorageFileRefCnt() = default;
  int init(const int64_t thread_cnt, const OptType type);
  virtual void run1();

private:
  int do_work(const MacroBlockId &macro_id);
  OptType type_;
  int64_t thread_cnt_;
};

TestStorageFileRefCnt::TestStorageFileRefCnt()
  : type_(OptType::MAX), thread_cnt_(0)
{
}

int TestStorageFileRefCnt::init(const int64_t thread_cnt, const OptType type)
{
  int ret = OB_SUCCESS;
  if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else {
    thread_cnt_ = thread_cnt;
    type_ = type;
    set_thread_count(static_cast<int32_t>(thread_cnt_));
  }
  return ret;
}

void TestStorageFileRefCnt::run1()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id(0, 1, 0);
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
    //STORAGE_LOG(WARN, "jinzhu debug", K(macro_id));
  }
  //STORAGE_LOG(WARN, "jinzhu debug", K(count));
}

int TestStorageFileRefCnt::do_work(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  switch(type_) {
  case OptType::INC:
    ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
    break;
  case OptType::DEC:
    ret =OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "type_ is an invalid argument");
  }
  return ret;
}

TEST_F(TestRefCnt, server_super_block)
{
  int ret = OB_SUCCESS;

  ObServerSuperBlock super_block;
  super_block.body_.tenant_meta_entry_ = MacroBlockId(0, 133, 0);
  super_block.body_.tenant_meta_entry_.set_write_seq(131);
  super_block.body_.replay_start_point_.file_id_ = 1;
  super_block.body_.replay_start_point_.log_id_ = 21923;
  super_block.body_.replay_start_point_.offset_ = 11497472;

  ret = super_block.format_startup_super_block(2 << 20, 2L << 30);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  int64_t pos = 0;
  char buf[64 << 10] = {0};
  ret = super_block.serialize(buf, sizeof(buf), pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(super_block.get_serialize_size(), pos);

  pos = 0;
  ObServerSuperBlock des_sb;
  ret = des_sb.deserialize(buf, sizeof(buf), pos);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(des_sb.get_serialize_size(), pos);

  ret = OB_SERVER_BLOCK_MGR.write_super_block(super_block);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObServerSuperBlock ret_sb;
  ret = OB_SERVER_BLOCK_MGR.read_super_block(ret_sb);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ASSERT_EQ(super_block.body_.macro_block_size_, ret_sb.body_.macro_block_size_);
  ASSERT_EQ(super_block.body_.total_file_size_, ret_sb.body_.total_file_size_);
}

TEST_F(TestRefCnt, test_inc_and_dec_ref)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id(0, 1, 0);

  int count = 10;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 9;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ObMacroBlockInfo block_info;
  ObMacroBlockHandle block_handle;
  ret = OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_id, block_info, block_handle);
  block_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);
}

TEST_F(TestRefCnt, test_multi_inc_and_dec_ref)
{
  int ret = OB_SUCCESS;

  TestStorageFileRefCnt inc_ref;
  TestStorageFileRefCnt dec_ref;
  ret = inc_ref.init(32, TestStorageFileRefCnt::OptType::INC);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = dec_ref.init(32, TestStorageFileRefCnt::OptType::DEC);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_ref.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  inc_ref.wait();
  ret = dec_ref.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  dec_ref.wait();
}

TEST_F(TestRefCnt, test_overmuch_dec_ref)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id(0, 1, 0);

  int count = 10;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 11;
  while (OB_SUCC(ret) && count--) {
    ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
  }
  ASSERT_EQ(OB_ERR_SYS, ret);
  ObMacroBlockInfo block_info;
  ObMacroBlockHandle block_handle;
  ret = OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_id, block_info, block_handle);
  block_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, block_info.ref_cnt_);
}

TEST_F(TestRefCnt, test_1_0_1)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id(0, 1, 0);

  int count = 10;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 9;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ObMacroBlockInfo block_info;
  ObMacroBlockHandle block_handle;
  ret = OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_id, block_info, block_handle);
  block_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);

  count = 9;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  count = 9;
  while (count--) {
    ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObMacroBlockHandle tmp_block_handle;
  ret = OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_id, block_info, tmp_block_handle);
  tmp_block_handle.reset();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);

  ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
  ASSERT_NE(OB_SUCCESS, OB_SERVER_BLOCK_MGR.inc_ref(macro_id));
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

  ret = OB_SERVER_FILE_MGR.alloc_storage_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = pg_file_->init(ObStorageFile::FileType::SERVER_ROOT, tenant_id);
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

  ret = OB_SERVER_FILE_MGR.free_storage_file(pg_file_);
  ASSERT_EQ(OB_SUCCESS, ret);
}*/

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ref_cnt.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ref_cnt.log", true);
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
