/**
 * Copyright (c) 2022 OceanBase
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

#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/storage/medium_info_helper.h"
#include "share/io/ob_io_manager.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

#define BUILD_AND_WRITE_MEDIUM_INFO(id) \
        { \
          compaction::ObMediumCompactionInfo *info = nullptr; \
          if (OB_FAIL(ret)) { \
          } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator_, info))) { \
            STORAGE_LOG(WARN, "failed to alloc and new", K(ret)); \
          } else { \
            if (OB_FAIL(MediumInfoHelper::build_medium_compaction_info(allocator_, *info, id))) { \
              STORAGE_LOG(WARN, "failed to build medium info", K(ret)); \
            } else { \
              const int64_t size = info->get_serialize_size(); \
              char *buffer = static_cast<char*>(allocator_.alloc(size)); \
              int64_t pos = 0; \
              if (OB_ISNULL(buffer)) { \
                ret = OB_ALLOCATE_MEMORY_FAILED; \
                STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(size)); \
              } else if (OB_FAIL(info->serialize(buffer, size, pos))) { \
                STORAGE_LOG(WARN, "failed to serialize", K(ret), K(size)); \
              } else { \
                ObSharedBlockWriteInfo write_info; \
                write_info.buffer_ = buffer; \
                write_info.offset_ = 0; \
                write_info.size_ = size; \
                write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE); \
                if (OB_FAIL(reader_writer.async_link_write(write_info, write_handle))) { \
                  STORAGE_LOG(WARN, "failed to async link write", K(ret)); \
                } \
              } \
            } \
            \
            if (OB_FAIL(ret)) { \
              if (nullptr != info) { \
                allocator_.free(info); \
              } \
            } \
          } \
        }

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
class TestTabletMdsData : public::testing::Test
{
public:
  TestTabletMdsData();
  virtual ~TestTabletMdsData() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  common::ObArenaAllocator allocator_; // for medium info
};

TestTabletMdsData::TestTabletMdsData()
  : allocator_()
{
}

void TestTabletMdsData::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTabletMdsData::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTabletMdsData::SetUp()
{
  int ret = OB_SUCCESS;
}

void TestTabletMdsData::TearDown()
{
}

TEST_F(TestTabletMdsData, read_medium_info)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReaderWriter reader_writer;
  ret = reader_writer.init(true/*need align*/, false/*need cross*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write
  ObSharedBlockLinkHandle write_handle;
  const int64_t current_time = ObTimeUtility::fast_current_time();
  BUILD_AND_WRITE_MEDIUM_INFO(current_time + 1);
  BUILD_AND_WRITE_MEDIUM_INFO(current_time + 2);
  BUILD_AND_WRITE_MEDIUM_INFO(current_time + 3);
  BUILD_AND_WRITE_MEDIUM_INFO(current_time + 4);
  BUILD_AND_WRITE_MEDIUM_INFO(current_time + 5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(write_handle.is_valid());

  // read
  ObSharedBlocksWriteCtx write_ctx;
  ObSharedBlockLinkIter iter;
  ret = write_handle.get_write_ctx(write_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);

  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> array;
  ret = ObTabletMdsData::read_medium_info(allocator_, write_ctx.addr_, array);
  ASSERT_EQ(OB_SUCCESS, ret);

  constexpr int64_t count = 5;
  for (int64_t i = 0; i < count; ++i) {
    const compaction::ObMediumCompactionInfo* info = array.at(i);
    ASSERT_EQ(current_time + count - i, info->medium_snapshot_);
  }
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_mds_data.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tablet_mds_data.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}