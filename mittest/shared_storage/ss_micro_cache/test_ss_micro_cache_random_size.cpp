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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheRandomSize : public ::testing::Test
{
public:
  TestSSMicroCacheRandomSize() {}
  virtual ~TestSSMicroCacheRandomSize() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  int parallel_add_and_get_micro_block(
      const int64_t micro_cnt, const int64_t min_micro_size, const int64_t max_micro_size, int64_t thread_id);
  int build_io_info(ObIOInfo &io_info, const uint64_t tenant_id, const ObSSMicroBlockCacheKey &micro_key,
      const int32_t size, char *read_buf);
};

void TestSSMicroCacheRandomSize::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheRandomSize::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheRandomSize::SetUp()
{}

void TestSSMicroCacheRandomSize::TearDown()
{}

int TestSSMicroCacheRandomSize::build_io_info(
    ObIOInfo &io_info,
    const uint64_t tenant_id,
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t size,
    char *read_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || size <= 0 || tenant_id == OB_INVALID_TENANT_ID) || OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(read_buf), K(size), K(tenant_id));
  } else {
    io_info.tenant_id_ = tenant_id;
    io_info.offset_ = micro_key.micro_id_.offset_;
    io_info.size_ = size;
    io_info.flag_.set_wait_event(1);
    io_info.timeout_us_ = 5 * 1000 * 1000;
    io_info.buf_ = read_buf;
    io_info.user_data_buf_ = read_buf;
    io_info.effective_tablet_id_ = micro_key.get_macro_tablet_id().id();
  }
  return ret;
}

int TestSSMicroCacheRandomSize::parallel_add_and_get_micro_block(
  const int64_t micro_cnt,
  const int64_t min_micro_size,
  const int64_t max_micro_size,
  int64_t thread_id)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_UNLIKELY(micro_cnt <= 0 || min_micro_size <= 0 || max_micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_cnt), K(min_micro_size), K(max_micro_size));
  } else {
    char *buf = nullptr;
    const int64_t payload_offset = ObSSMemBlock::get_reserved_size();

    for (int64_t i = 0; OB_SUCC(ret) && i < micro_cnt; ++i) {
      const int32_t micro_size = ObRandom::rand(min_micro_size, max_micro_size);
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id((thread_id + 1) * micro_cnt + i);
      char c = macro_id.hash() % 26 + 'a';
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(micro_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
      } else {
        MEMSET(buf, c, micro_size);
        const int32_t offset = payload_offset;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        micro_cache->add_micro_block_cache(micro_key, buf, micro_size,
                                           macro_id.second_id()/*effective_tablet_id*/,
                                           ObSSMicroCacheAccessType::COMMON_IO_TYPE);

        { // check data
          const int64_t persist_task_interval_us = micro_cache->task_runner_.persist_data_task_.ori_interval_us_;
          const int64_t random_sleep_us = ObRandom::rand(persist_task_interval_us / 20, persist_task_interval_us);
          usleep(random_sleep_us);

          ObSSMicroBlockId ss_micro_block_id(macro_id, offset, micro_size);
          ObIOInfo io_info;
          ObStorageObjectHandle obj_handle;
          MEMSET(buf, '\0', micro_size);
          bool is_hit = false;
          if (OB_FAIL(build_io_info(io_info, MTL_ID(), micro_key, micro_size, buf))) {
            LOG_WARN("fail to build io_info", KR(ret), KP(buf));
          } else if (OB_FAIL(micro_cache->get_micro_block_cache(micro_key, ss_micro_block_id, ObSSMicroCacheGetType::FORCE_GET_DATA,
                     io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE, is_hit))) {
            LOG_WARN("fail to get_micro_block_cache", KR(ret), K(ss_micro_block_id));
          } else if (OB_FAIL(obj_handle.wait())) {
            LOG_WARN("fail to wait until get micro block data", KR(ret), K(micro_key));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < micro_size; ++j) {
              if (OB_ISNULL(obj_handle.get_buffer())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("buffer should not be null", KR(ret), K(j), K(micro_size), K(obj_handle));
              } else if (obj_handle.get_buffer()[j] != c) {
                ret = OB_IO_ERROR;
                LOG_WARN("data error", KR(ret), K(i), K(j), K(micro_key), K(c), K(obj_handle.get_buffer()));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

TEST_F(TestSSMicroCacheRandomSize, test_add_and_get_with_random_size)
{
  const int64_t thread_num = 2;
  const int64_t min_micro_size = 200;
  const int64_t max_micro_size = 500;
  const int64_t micro_cnt = 10000;

  ObTenantBase *tenant_base = MTL_CTX();
  auto test_func = [&](const int64_t idx) {
    ObTenantEnv::set_tenant(tenant_base);
    ASSERT_EQ(OB_SUCCESS, parallel_add_and_get_micro_block(micro_cnt, min_micro_size, max_micro_size, idx));
  };
  std::vector<std::thread> ths;
  for (int64_t i = 0; i < thread_num; ++i) {
    std::thread th(test_func, i);
    ths.push_back(std::move(th));
  }
  for (int64_t i = 0; i < thread_num; ++i) {
    ths[i].join();
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_random_size.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_random_size.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
