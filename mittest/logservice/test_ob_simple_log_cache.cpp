// owner: liuhanyi.lhy
// owner group: log

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

#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#undef protected


const std::string TEST_NAME = "log_cache";

using namespace oceanbase::common;
using namespace oceanbase;

namespace oceanbase
{
using namespace logservice;

namespace palf
{
int LogColdCache::allow_filling_cache_(LogIteratorInfo *iterator_info, bool &enable_fill_cache)
{
  int ret = OB_SUCCESS;
  enable_fill_cache = true;
  return ret;
}
}
namespace unittest
{
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;
class TestObSimpleLogCache : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogCache() : ObSimpleLogClusterTestEnv() {
    int ret = init();
    if (OB_SUCCESS != ret) {
      throw std::runtime_error("TestObSimpleLogCache init failed");
    }
  }
  ~TestObSimpleLogCache() { destroy(); }
  int init() {
    return OB_SUCCESS;
  }
  void destroy() {}
  using ObSimpleLogClusterTestEnv::read_log;
  int read_log(PalfHandleImplGuard &leader, PalfBufferIterator &iterator, LogIOUser user)
  {
    return read_log(leader, LSN(PALF_INITIAL_LSN_VAL), iterator, user);
  }
  int read_log(PalfHandleImplGuard &leader, LSN read_lsn, PalfBufferIterator &iterator, LogIOUser user)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(leader.palf_handle_impl_->alloc_palf_buffer_iterator(read_lsn, iterator))) {
    } else if (OB_FAIL(iterator.set_io_context(palf::LogIOContext(MTL_ID(), id_, user)))) {
      PALF_LOG(WARN, "set_io_context failed", K(read_lsn), K(user));
    } else {
      while (OB_SUCCESS == ret) {
        const char *buf;
        int64_t buf_len = 0;
        share::SCN scn = share::SCN::min_scn();
        LSN log_offset;
        bool is_raw_write = false;
        if (OB_FAIL(iterator.next())) {
        } else if (OB_FAIL(iterator.get_entry(buf, buf_len, scn, log_offset, is_raw_write))) {
        } else {
          PALF_LOG(TRACE, "print log entry", K(is_raw_write), K(iterator));
        }
      }
    }
    return ret;
  }
  int64_t id_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;

TEST_F(TestObSimpleLogCache, read)
{
  update_server_log_disk(10*1024*1024*1024ul);
  update_disk_options(10*1024*1024*1024ul/palf::PALF_PHY_BLOCK_SIZE);
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "read");
  OB_LOGGER.set_log_level("TRACE");
  int server_idx = 0;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t total_used_size = 0, total_size = 0;
  share::SCN create_scn = share::SCN::base_scn();
  PALF_LOG(INFO, "start to test log cache", K(id));

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 5000, 30 * 1024, id, lsn_array, scn_array));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  PALF_LOG(INFO, "first to read log");

  PalfBufferIterator iterator1;
  EXPECT_EQ(OB_ITER_END, read_log(leader, iterator1, LogIOUser::FETCHLOG));
  EXPECT_EQ(false, iterator1.io_ctx_.iterator_info_.allow_filling_cache_);

  PalfBufferIterator iterator2;
  EXPECT_EQ(OB_ITER_END, read_log(leader, iterator2, LogIOUser::CDC));
  EXPECT_EQ(true, iterator2.io_ctx_.iterator_info_.allow_filling_cache_);

  OB_LOG_KV_CACHE.destroy();
}

TEST_F(TestObSimpleLogCache, concurrent_read)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "concurrent_read");
  OB_LOGGER.set_log_level("TRACE");
  int server_idx = 0;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start to test log cache", K(id));

  EXPECT_EQ(OB_SUCCESS, OB_LOG_KV_CACHE.init(OB_LOG_KV_CACHE_NAME, 1));
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, MAX_LOG_BODY_SIZE, id, lsn_array, scn_array));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  {
    std::thread read_thread_1([this, &leader, &leader_idx] {
      ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
      EXPECT_EQ(OB_ITER_END, read_log(leader));
    });

    std::thread read_thread_2([this, &lsn_array, &leader, &leader_idx] {
      ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
      PALF_LOG(INFO, "start to hit cache");
      LSN read_lsn(lsn_array[2]);
      EXPECT_EQ(OB_ITER_END, read_log(leader));
    });
    read_thread_1.join();
    read_thread_2.join();
  }

  OB_LOG_KV_CACHE.destroy();
}

TEST_F(TestObSimpleLogCache, raw_read)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "raw_read");
  OB_LOGGER.set_log_level("TRACE");
  int server_idx = 0;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "start to test log cache", K(id));

  EXPECT_EQ(OB_SUCCESS, OB_LOG_KV_CACHE.init(OB_LOG_KV_CACHE_NAME, 1));
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, MAX_LOG_BODY_SIZE, id, lsn_array, scn_array));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));
  int64_t aligned_offset = common::upper_align(lsn_array[35].val_, LOG_DIO_ALIGN_SIZE);
  LSN aligned_lsn(aligned_offset);
  int64_t in_read_size = 2 * 1024 * 1024;
  int64_t out_read_size = 0;
  char *read_buf = reinterpret_cast<char*>(mtl_malloc_align(
    LOG_DIO_ALIGN_SIZE, in_read_size, "mittest"));
  LogIOContext ctx;
  EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->raw_read(aligned_lsn, read_buf, in_read_size, out_read_size, ctx));

  if (OB_NOT_NULL(read_buf)) {
    mtl_free_align(read_buf);
  }
  OB_LOG_KV_CACHE.destroy();
}

TEST_F(TestObSimpleLogCache, fill_cache_when_slide)
{
  disable_hot_cache_ = false;
  SET_CASE_LOG_FILE(TEST_NAME, "fill_cache_when_slide");
  OB_LOGGER.set_log_level("TRACE");
  int server_idx = 0;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t total_used_size = 0, total_size = 0;
  share::SCN create_scn = share::SCN::base_scn();
  PALF_LOG(INFO, "start to test log cache", K(id));

  EXPECT_EQ(OB_SUCCESS, OB_LOG_KV_CACHE.init(OB_LOG_KV_CACHE_NAME, 1));
  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  {
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 5000, 30 * 1024, id, lsn_array, scn_array));
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));

    PalfBufferIterator iterator;
    PALF_LOG(INFO, "start to read log");
    EXPECT_EQ(OB_ITER_END, read_log(leader, iterator, LogIOUser::CDC));
  }

  {
    PALF_LOG(INFO, "test exceptional situations: miss hot cache", K(id));
    // miss hot cache when committed logs slide, unable to fill cold cache
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 2000, 1024, id, lsn_array, scn_array));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
    PALF_LOG(INFO, "reset hot cache", K(id));
    leader.get_palf_handle_impl()->log_engine_.log_storage_.log_cache_->hot_cache_.reset();

    LSN failed_aligned_lsn = leader.get_palf_handle_impl()->log_engine_.log_storage_.log_cache_->fill_buf_.aligned_lsn_;
    const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
    SCN result_scn;
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->locate_by_lsn_coarsely(failed_aligned_lsn, result_scn));
    LSN read_lsn;
    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->locate_by_scn_coarsely(result_scn, read_lsn));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, MAX_LOG_BODY_SIZE, id, lsn_array, scn_array));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));

    EXPECT_EQ(OB_SUCCESS, leader.get_palf_handle_impl()->log_engine_.log_storage_.log_cache_->hot_cache_.init(id, leader.get_palf_handle_impl()));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 2000, 1024, id, lsn_array, scn_array));

    PalfBufferIterator iterator;

    EXPECT_EQ(OB_ITER_END, read_log(leader, read_lsn, iterator, LogIOUser::CDC));
    // miss, have to read disk at lease once
    EXPECT_LT(0, iterator.io_ctx_.iterator_info_.cold_cache_stat_.miss_cnt_);
  }


  OB_LOG_KV_CACHE.destroy();
}


} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
