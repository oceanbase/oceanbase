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
#include "test_ss_common_util.h"
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

public:
  struct TestSSMicroCacheRandomSizeCtx
  {
  public:
    ObSSMicroCache *micro_cache_;
    uint64_t tenant_id_;
    int64_t min_micro_size_;
    int64_t max_micro_size_;
    int64_t micro_cnt_;
    bool check_read_data_;

    TestSSMicroCacheRandomSizeCtx() { reset(); }
    bool is_valid() const { return (nullptr != micro_cache_) && (min_micro_size_ > 0) && (max_micro_size_ >= min_micro_size_)
                                   && (micro_cnt_ > 0); }
    void reset()
    {
      micro_cache_ = nullptr;
      tenant_id_ = OB_INVALID_TENANT_ID;
      min_micro_size_ = 0;
      max_micro_size_ = 0;
      micro_cnt_ = 0;
      check_read_data_ = false;
    }

    TO_STRING_KV(KP_(micro_cache), K_(tenant_id), K_(min_micro_size), K_(max_micro_size), K_(micro_cnt), K_(check_read_data));
  };

  class TestSSMicroCacheRandomSizeThread : public Threads
  {
  public:
    TestSSMicroCacheRandomSizeThread(ObTenantBase *tenant_base, TestSSMicroCacheRandomSizeCtx &ctx)
        : tenant_base_(tenant_base), ctx_(ctx), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int build_io_info(ObIOInfo &io_info, const uint64_t tenant_id, const ObSSMicroBlockCacheKey &micro_key,
                      const int32_t size, char *read_buf);
    int parallel_add_and_get_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheRandomSizeCtx &ctx_;
    int64_t fail_cnt_;
  };
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

void TestSSMicroCacheRandomSize::TestSSMicroCacheRandomSizeThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  parallel_add_and_get_micro_block(idx);
}

int TestSSMicroCacheRandomSize::TestSSMicroCacheRandomSizeThread::build_io_info(
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
  }
  return ret;
}

int TestSSMicroCacheRandomSize::TestSSMicroCacheRandomSizeThread::parallel_add_and_get_micro_block(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ctx));
  } else {
    char *buf = nullptr;
    const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                   ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.micro_cnt_; ++i) {
      const int32_t micro_size = ObRandom::rand(ctx_.min_micro_size_, ctx_.max_micro_size_);
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id((idx + 1) * ctx_.micro_cnt_ + i);
      char c = macro_id.hash() % 26 + 'a';
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(micro_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
      } else {
        MEMSET(buf, c, micro_size);
        const int32_t offset = payload_offset;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        ctx_.micro_cache_->add_micro_block_cache(micro_key, buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);

        if (ctx_.check_read_data_) {
          const int64_t persist_task_interval_us = ctx_.micro_cache_->task_runner_.persist_task_.interval_us_;
          const int64_t random_sleep_us = ObRandom::rand(persist_task_interval_us / 20, persist_task_interval_us);
          usleep(random_sleep_us);

          ObSSMicroBlockId ss_micro_block_id(macro_id, offset, micro_size);
          ObIOInfo io_info;
          ObStorageObjectHandle obj_handle;
          MEMSET(buf, '\0', micro_size);
          if (OB_FAIL(build_io_info(io_info, ctx_.tenant_id_, micro_key, micro_size, buf))) {
            LOG_WARN("fail to build io_info", KR(ret), K_(ctx), KP(buf));
          } else if (OB_FAIL(ctx_.micro_cache_->get_micro_block_cache(micro_key, ss_micro_block_id,
                     MicroCacheGetType::FORCE_GET_DATA, io_info, obj_handle,
                     ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
            LOG_WARN("fail to get_micro_block_cache", KR(ret), K(ss_micro_block_id));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < micro_size; ++j) {
              if (obj_handle.get_buffer()[j] != c) {
                ret = OB_IO_ERROR;
                LOG_WARN("data error", KR(ret), K(i), K(j), K(micro_key), K(c), K(obj_handle.get_buffer()));
              }
            }
          }
        }
      }
      allocator.clear();
    }
  }

  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroCacheRandomSize, test_add_and_get_with_random_size)
{
  const int64_t thread_num = 2;
  TestSSMicroCacheRandomSize::TestSSMicroCacheRandomSizeCtx ctx;
  ctx.micro_cache_ = MTL(ObSSMicroCache *);
  ctx.tenant_id_ = MTL_ID();
  ctx.min_micro_size_ = 200;
  ctx.max_micro_size_ = 500;
  ctx.micro_cnt_ = 10000;
  ctx.check_read_data_ = true;

  TestSSMicroCacheRandomSize::TestSSMicroCacheRandomSizeThread threads(ObTenantEnv::get_tenant(), ctx);
  threads.set_thread_count(thread_num);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
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
