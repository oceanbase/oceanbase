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

#define protected public
#define private public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class TestMdsDataReadWrite : public::testing::Test
{
public:
  TestMdsDataReadWrite();
  virtual ~TestMdsDataReadWrite() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  //virtual void SetUp() override;
  //virtual void TearDown() override;
private:
  static int mock_tablet_status_disk_addr(
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &addr);
  static int mock_empty_tablet_status_disk_addr(
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &addr);
  static int mock_auto_inc_seq_disk_addr(
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &addr);
  static int mock_empty_auto_inc_seq_disk_addr(
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &addr);
};

TestMdsDataReadWrite::TestMdsDataReadWrite()
{
}

void TestMdsDataReadWrite::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestMdsDataReadWrite::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

int TestMdsDataReadWrite::mock_tablet_status_disk_addr(
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  mds::MdsDumpKV kv;

  {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    const int64_t size = user_data.get_serialize_size();
    int64_t pos = 0;
    char *buf = static_cast<char*>(allocator.alloc(size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(size));
    } else if (OB_FAIL(user_data.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    } else {
      ObString &str = kv.v_.user_data_;
      str.assign(buf, size);
    }
  }


  const int64_t size = kv.get_serialize_size();
  int64_t pos = 0;

  char *buf = static_cast<char*>(allocator.alloc(size));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(kv.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    ObSharedBlockWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);

    ObSharedBlockWriteHandle handle;
    ObSharedBlocksWriteCtx write_ctx;
    if (OB_FAIL(ckpt_slog_handler->get_shared_block_reader_writer().async_write(write_info, handle))) {
      LOG_WARN("failed to do async write", K(ret));
    } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret));
    } else {
      addr = write_ctx.addr_;
    }
  }

  if (nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

int TestMdsDataReadWrite::mock_empty_tablet_status_disk_addr(
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  mds::MdsDumpKV kv;
  const int64_t size = kv.get_serialize_size();
  int64_t pos = 0;

  char *buf = static_cast<char*>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(kv.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    ObSharedBlockWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);

    ObSharedBlockWriteHandle handle;
    ObSharedBlocksWriteCtx write_ctx;
    if (OB_FAIL(ckpt_slog_handler->get_shared_block_reader_writer().async_write(write_info, handle))) {
      LOG_WARN("failed to do async write", K(ret));
    } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret));
    } else {
      addr = write_ctx.addr_;
    }
  }

  if (nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

int TestMdsDataReadWrite::mock_auto_inc_seq_disk_addr(
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  share::ObTabletAutoincSeq auto_inc_seq;

  {
    if (OB_FAIL(auto_inc_seq.set_autoinc_seq_value(allocator, 100))) {
      LOG_WARN("failed to set auto inc seq", K(ret));
    }
  }

  const int64_t size = auto_inc_seq.get_serialize_size();
  int64_t pos = 0;

  char *buf = static_cast<char*>(allocator.alloc(size));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(auto_inc_seq.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    ObSharedBlockWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);

    ObSharedBlockWriteHandle handle;
    ObSharedBlocksWriteCtx write_ctx;
    if (OB_FAIL(ckpt_slog_handler->get_shared_block_reader_writer().async_write(write_info, handle))) {
      LOG_WARN("failed to do async write", K(ret));
    } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret));
    } else {
      addr = write_ctx.addr_;
    }
  }

  if (nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

int TestMdsDataReadWrite::mock_empty_auto_inc_seq_disk_addr(
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_handler = MTL(ObTenantCheckpointSlogHandler*);
  share::ObTabletAutoincSeq auto_inc_seq;
  const int64_t size = auto_inc_seq.get_serialize_size();
  int64_t pos = 0;

  char *buf = static_cast<char*>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(size));
  } else if (OB_FAIL(auto_inc_seq.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else {
    ObSharedBlockWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);

    ObSharedBlockWriteHandle handle;
    ObSharedBlocksWriteCtx write_ctx;
    if (OB_FAIL(ckpt_slog_handler->get_shared_block_reader_writer().async_write(write_info, handle))) {
      LOG_WARN("failed to do async write", K(ret));
    } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret));
    } else {
      addr = write_ctx.addr_;
    }
  }

  if (nullptr != buf) {
    allocator.free(buf);
  }

  return ret;
}

TEST_F(TestMdsDataReadWrite, mds_dump_kv)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  ObTabletComplexAddr<mds::MdsDumpKV> src_addr;
  ObTabletComplexAddr<mds::MdsDumpKV> dst_addr;

  {
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  {
    src_addr.reset();
    dst_addr.reset();
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, src_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    src_addr.addr_.set_none_addr();
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();

    ObMetaDiskAddr addr;
    ret = TestMdsDataReadWrite::mock_empty_tablet_status_disk_addr(allocator, addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(addr.is_block());
    src_addr.ptr_ = nullptr;
    src_addr.addr_ = addr;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();

    ObMetaDiskAddr addr;
    ret = TestMdsDataReadWrite::mock_tablet_status_disk_addr(allocator, addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(addr.is_block());
    src_addr.ptr_ = nullptr;
    src_addr.addr_ = addr;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_memory_object());
  }
}

TEST_F(TestMdsDataReadWrite, auto_inc_seq)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  ObTabletComplexAddr<share::ObTabletAutoincSeq> src_addr;
  ObTabletComplexAddr<share::ObTabletAutoincSeq> dst_addr;

  {
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  {
    src_addr.reset();
    dst_addr.reset();
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, src_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    src_addr.addr_.set_none_addr();
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();

    ObMetaDiskAddr addr;
    ret = TestMdsDataReadWrite::mock_empty_auto_inc_seq_disk_addr(allocator, addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(addr.is_block());
    src_addr.ptr_ = nullptr;
    src_addr.addr_ = addr;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();

    ObMetaDiskAddr addr;
    ret = TestMdsDataReadWrite::mock_auto_inc_seq_disk_addr(allocator, addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(addr.is_block());
    src_addr.ptr_ = nullptr;
    src_addr.addr_ = addr;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_memory_object());
  }
}

TEST_F(TestMdsDataReadWrite, medium_info_list)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  ObTabletComplexAddr<ObTabletDumpedMediumInfo> src_addr;
  ObTabletComplexAddr<ObTabletDumpedMediumInfo> dst_addr;

  {
    const int64_t finish_medium_scn = 0;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, 0, dst_addr);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  {
    src_addr.reset();
    dst_addr.reset();
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, src_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = src_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    src_addr.addr_.set_none_addr();

    const int64_t finish_medium_scn = 0;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, finish_medium_scn, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, src_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = src_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    src_addr.addr_.set_none_addr();

    compaction::ObMediumCompactionInfo medium_info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator, medium_info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = src_addr.ptr_->append(medium_info);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t finish_medium_scn = 80;
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, finish_medium_scn, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_memory_object());

    finish_medium_scn = 200;
    dst_addr.reset();
    ret = ObTabletMdsData::init_single_complex_addr(allocator, src_addr, finish_medium_scn, dst_addr);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }

  {
    src_addr.reset();
    dst_addr.reset();
    ret = ObTabletObjLoadHelper::alloc_and_new(allocator, src_addr.ptr_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = src_addr.ptr_->init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    src_addr.addr_.set_none_addr();

    compaction::ObMediumCompactionInfo medium_info;
    ret = MediumInfoHelper::build_medium_compaction_info(allocator, medium_info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = src_addr.ptr_->append(medium_info);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTabletDumpedMediumInfo src_data;
    ret = src_data.init_for_first_creation(allocator);
    ASSERT_EQ(OB_SUCCESS, ret);
    medium_info.medium_snapshot_ = 120;
    ret = src_data.append(medium_info);
    ASSERT_EQ(OB_SUCCESS, ret);

    compaction::ObExtraMediumInfo src_addr_extra_info;
    src_addr_extra_info.last_medium_scn_ = 10;
    compaction::ObExtraMediumInfo src_data_extra_info;
    src_data_extra_info.last_medium_scn_ = 20;
    compaction::ObExtraMediumInfo dst_extra_info;

    int64_t finish_medium_scn = 80;
    ret = ObTabletMdsData::init_single_complex_addr_and_extra_info(allocator, src_addr, src_addr_extra_info,
        src_data, src_data_extra_info, finish_medium_scn, dst_addr, dst_extra_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_memory_object());
    ASSERT_EQ(2, dst_addr.ptr_->medium_info_list_.count());

    finish_medium_scn = 110;
    ret = ObTabletMdsData::init_single_complex_addr_and_extra_info(allocator, src_addr, src_addr_extra_info,
        src_data, src_data_extra_info, finish_medium_scn, dst_addr, dst_extra_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_memory_object());
    ASSERT_EQ(1, dst_addr.ptr_->medium_info_list_.count());

    finish_medium_scn = 130;
    ret = ObTabletMdsData::init_single_complex_addr_and_extra_info(allocator, src_addr, src_addr_extra_info,
        src_data, src_data_extra_info, finish_medium_scn, dst_addr, dst_extra_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(dst_addr.is_none_object());
  }
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_data_read_write.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_mds_data_read_write.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}