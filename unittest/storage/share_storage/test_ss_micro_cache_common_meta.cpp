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
#include <thread>
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/config/ob_config_helper.h"
#include "share/shared_storage/ob_ss_local_cache_control_mode.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheCommonMeta : public ::testing::Test
{
public:
  TestSSMicroCacheCommonMeta();
  virtual ~TestSSMicroCacheCommonMeta();
  virtual void SetUp();
  virtual void TearDown();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

struct MockSSCkptItemHeader
{
public:
  int32_t payload_size_;
  MockSSCkptItemHeader() : payload_size_(0) {}
  ~MockSSCkptItemHeader() {}
  static const int64_t MOCK_VERSION = 1;
  OB_UNIS_VERSION(MOCK_VERSION);
};

OB_SERIALIZE_MEMBER(MockSSCkptItemHeader, payload_size_);

struct MockSSCkptItemChildHeader
{
public:
  int32_t payload_size_;
  int32_t crc_;
  MockSSCkptItemChildHeader() : payload_size_(0), crc_(0) {}
  ~MockSSCkptItemChildHeader() {}
  static const int64_t MOCK_VERSION = 1;
  OB_UNIS_VERSION(MOCK_VERSION);
};

OB_SERIALIZE_MEMBER(MockSSCkptItemChildHeader, payload_size_, crc_);

TestSSMicroCacheCommonMeta::TestSSMicroCacheCommonMeta()
{}

TestSSMicroCacheCommonMeta::~TestSSMicroCacheCommonMeta()
{}

void TestSSMicroCacheCommonMeta::SetUp()
{}

void TestSSMicroCacheCommonMeta::TearDown()
{}

TEST_F(TestSSMicroCacheCommonMeta, ss_micro_block_id)
{
  MacroBlockId macro_id(0, 200, 0);
  ObSSMicroBlockId ss_micro_id(macro_id, 0, 0);
  ASSERT_EQ(false, ss_micro_id.is_valid());
  ss_micro_id.offset_ = 100;
  ss_micro_id.size_ = 200;
  ASSERT_EQ(true, ss_micro_id.is_valid());

  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ss_micro_id.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, ss_micro_id.get_serialize_size());
  ObSSMicroBlockId tmp_ss_micro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_ss_micro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, ss_micro_id == tmp_ss_micro_id);
}

TEST_F(TestSSMicroCacheCommonMeta, ss_micro_block_key)
{
  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  // 1. serialize and deserialize logic_macro_id
  ObLogicMacroBlockId logic_macro_id(32, 200, 300);
  ASSERT_EQ(true, logic_macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, logic_macro_id.serialize(buf, buf_size, pos));
  ObLogicMacroBlockId tmp_logic_macro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_logic_macro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_logic_macro_id, logic_macro_id);

  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.logic_macro_id_ = logic_macro_id;
  logic_micro_id.logic_macro_id_.info_ = 400;
  logic_micro_id.version_ = 1;
  logic_micro_id.offset_ = 123;
  ASSERT_EQ(true, logic_micro_id.is_valid());

  // 2. serialize and deserialize logic_micro_id
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, logic_micro_id.serialize(buf, buf_size, pos));
  ObLogicMicroBlockId tmp_micro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_micro_id, logic_micro_id);
  ASSERT_EQ(400, tmp_micro_id.logic_macro_id_.info_);
  ASSERT_EQ(1, tmp_micro_id.version_);
  ASSERT_EQ(123, tmp_micro_id.offset_);

  // 3. serialize and deserialize for LOGICAL mode
  ObSSMicroBlockCacheKey ss_micro_key;
  ss_micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
  ss_micro_key.logic_micro_id_ = logic_micro_id;
  ss_micro_key.micro_crc_ = 99;
  pos = 0;
  ASSERT_EQ(true, ss_micro_key.is_valid());
  ASSERT_EQ(OB_SUCCESS, ss_micro_key.serialize(buf, buf_size, pos));
  ObSSMicroBlockCacheKey tmp_micro_key;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_micro_key.is_valid());
  ASSERT_EQ(ss_micro_key, tmp_micro_key);
  ASSERT_EQ(400, tmp_micro_key.logic_micro_id_.logic_macro_id_.info_);
  ASSERT_EQ(1, tmp_micro_key.logic_micro_id_.version_);
  ASSERT_EQ(123, tmp_micro_key.logic_micro_id_.offset_);
  ASSERT_EQ(99, tmp_micro_key.micro_crc_);
  tmp_micro_key.reset();
  ASSERT_EQ(false, tmp_micro_key.is_valid());

  // 4. serialize and deserialize for PHYSICAL mode
  ObSSMicroBlockCacheKey ss_micro_key2;
  ss_micro_key2.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  blocksstable::MacroBlockId macro_id(0, 201, 0);
  ss_micro_key2.micro_id_.macro_id_ = macro_id;
  ss_micro_key2.micro_id_.offset_ = 127;
  ss_micro_key2.micro_id_.size_ = 231;

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ss_micro_key2.serialize(buf, buf_size, pos));

  ObSSMicroBlockCacheKey tmp_micro_key2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key2.deserialize(buf, buf_size, pos));
  ASSERT_EQ(ss_micro_key2, tmp_micro_key2);
  ASSERT_EQ(231, tmp_micro_key2.micro_id_.size_);
  ASSERT_EQ(127, tmp_micro_key2.micro_id_.offset_);
}

TEST_F(TestSSMicroCacheCommonMeta, super_block)
{
  const int64_t file_size = 1 << 30;
  const uint64_t tenant_id = 1;
  const int64_t micro_ckpt_split_cnt = 2;
  ObSSMicroCacheSuperBlk super_blk(tenant_id, file_size, micro_ckpt_split_cnt);
  super_blk.micro_ckpt_time_us_ = 10001;
  ASSERT_EQ(true, super_blk.is_valid());
  ASSERT_EQ(false, super_blk.micro_ckpt_info_.check_init(micro_ckpt_split_cnt + 1));
  ASSERT_EQ(true, super_blk.micro_ckpt_info_.check_init(micro_ckpt_split_cnt));
  ASSERT_EQ(micro_ckpt_split_cnt, super_blk.get_ckpt_split_cnt());

  ASSERT_EQ(file_size, super_blk.cache_file_size_);
  super_blk.blk_ckpt_info_.blk_ckpt_entry_ = 5;
  ASSERT_EQ(OB_SUCCESS, super_blk.blk_ckpt_used_blk_list().push_back(5));
  ASSERT_EQ(OB_SUCCESS, super_blk.blk_ckpt_used_blk_list().push_back(15));
  ASSERT_EQ(OB_SUCCESS, super_blk.blk_ckpt_used_blk_list().push_back(25));
  super_blk.micro_ckpt_entries().at(0).set_info(true, 100);
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_used_blk_list_by_idx(0).push_back(100));
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_used_blk_list_by_idx(0).push_back(101));
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_used_blk_list_by_idx(0).push_back(102));
  super_blk.micro_ckpt_entries().at(1).set_info(true, 200);
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_used_blk_list_by_idx(1).push_back(200));
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_used_blk_list_by_idx(1).push_back(201));
  super_blk.micro_ckpt_entries().at(0).set_info(true, SS_INVALID_PHY_BLK_ID);
  ASSERT_EQ(true, super_blk.exist_micro_checkpoint());
  super_blk.micro_ckpt_entries().at(1).set_info(true, SS_INVALID_PHY_BLK_ID);
  ASSERT_EQ(false, super_blk.exist_micro_checkpoint());
  super_blk.micro_ckpt_entries().at(0).set_info(true, 100);
  super_blk.micro_ckpt_entries().at(1).set_info(true, 200);
  ASSERT_EQ(true, super_blk.exist_micro_checkpoint());

  ObSSMicroCacheSuperBlk tmp_super_blk;
  ASSERT_EQ(false, tmp_super_blk.micro_ckpt_info_.check_init(micro_ckpt_split_cnt));
  ASSERT_EQ(OB_SUCCESS, tmp_super_blk.blk_ckpt_used_blk_list().push_back(6));
  ASSERT_EQ(OB_SUCCESS, tmp_super_blk.micro_ckpt_entries().push_back(ObSSMicroCkptEntryItem(true, 300)));
  ObSEArray<int64_t, 32> tmp_blk_list;
  ASSERT_EQ(OB_SUCCESS, tmp_blk_list.push_back(300));
  ASSERT_EQ(OB_SUCCESS, tmp_super_blk.micro_ckpt_info_.micro_ckpt_used_blks_.push_back(tmp_blk_list));

  ASSERT_EQ(OB_SUCCESS, tmp_super_blk.assign(super_blk));
  ASSERT_EQ(true, tmp_super_blk.micro_ckpt_info_.check_init(micro_ckpt_split_cnt));
  ASSERT_EQ(tenant_id, tmp_super_blk.tenant_id_);
  ASSERT_EQ(3, tmp_super_blk.blk_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(5, tmp_super_blk.micro_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(super_blk.micro_ckpt_time_us_, tmp_super_blk.micro_ckpt_time_us_);
  ASSERT_EQ(super_blk.cache_file_size_, tmp_super_blk.cache_file_size_);
  ASSERT_EQ(super_blk.get_ckpt_split_cnt(), tmp_super_blk.get_ckpt_split_cnt());

  // serialize & deserialize
  const int64_t buf_len = 1024;
  char buf[buf_len];
  MEMSET(buf, '\0', buf_len);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_blk.serialize(buf, buf_len, pos));
  ASSERT_NE(0, pos);
  const int64_t data_len = super_blk.get_serialize_size();

  tmp_super_blk.reset();
  int64_t tmp_pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_super_blk.deserialize(buf, data_len, tmp_pos));
  ASSERT_EQ(file_size, tmp_super_blk.cache_file_size_);
  ASSERT_EQ(micro_ckpt_split_cnt, tmp_super_blk.get_ckpt_split_cnt());
  ASSERT_EQ(pos, tmp_pos);
  ASSERT_EQ(5, tmp_super_blk.blk_ckpt_info_.blk_ckpt_entry_);
  ASSERT_EQ(2, tmp_super_blk.micro_ckpt_info_.micro_ckpt_entries_.count());
  ASSERT_EQ(true, tmp_super_blk.micro_ckpt_info_.micro_ckpt_entries_.at(0).exist_ckpt_);
  ASSERT_EQ(100, tmp_super_blk.micro_ckpt_info_.micro_ckpt_entries_.at(0).entry_blk_idx_);
  ASSERT_EQ(true, tmp_super_blk.micro_ckpt_info_.micro_ckpt_entries_.at(1).exist_ckpt_);
  ASSERT_EQ(200, tmp_super_blk.micro_ckpt_info_.micro_ckpt_entries_.at(1).entry_blk_idx_);

  tmp_super_blk.reuse();
  ASSERT_EQ(0, tmp_super_blk.blk_ckpt_info_.get_total_used_blk_cnt());
  ASSERT_EQ(0, tmp_super_blk.micro_ckpt_info_.get_total_used_blk_cnt());
}

TEST_F(TestSSMicroCacheCommonMeta, ls_tablet_cache_info)
{
  ObSSMicroCacheTabletInfo tablet_cache_info;
  ASSERT_EQ(false, tablet_cache_info.is_valid());
  ASSERT_EQ(0, tablet_cache_info.get_valid_size());
  ObTabletID tablet_id(101);
  tablet_cache_info.tablet_id_ = tablet_id;
  ASSERT_EQ(true, tablet_cache_info.is_valid());
  tablet_cache_info.add_micro_size(true, false, 100);
  tablet_cache_info.add_micro_size(false, false, 200);
  ASSERT_EQ(300, tablet_cache_info.get_valid_size());
  tablet_cache_info.add_micro_size(false, true, 300);
  ASSERT_EQ(300, tablet_cache_info.get_valid_size());

  ObSSMicroCacheTabletInfo tmp_tablet_cache_info = tablet_cache_info;
  ASSERT_EQ(true, tmp_tablet_cache_info.is_valid());
  ASSERT_EQ(300, tmp_tablet_cache_info.get_valid_size());

  ObSSMicroCacheLSInfo ls_cache_info;
  ASSERT_EQ(false, ls_cache_info.is_valid());
  ASSERT_EQ(0, ls_cache_info.get_valid_size());
  share::ObLSID ls_id(102);
  ls_cache_info.ls_id_ = ls_id;
  ASSERT_EQ(true, ls_cache_info.is_valid());
  ls_cache_info.t1_size_ = 200;
  ls_cache_info.t2_size_ = 300;
  ASSERT_EQ(500, ls_cache_info.get_valid_size());
}

TEST_F(TestSSMicroCacheCommonMeta, expired_micro_meta)
{
  ObSSMicroBlockMeta micro_meta;
  micro_meta.ref_cnt_ = 10;

  micro_meta.update_newest_access_time_s();
  micro_meta.access_time_s_ -= 4 * 3600;
  ASSERT_EQ(false, micro_meta.is_expired(SS_DEF_CACHE_EXPIRATION_TIME_S));
  micro_meta.access_time_s_ -= 44 * 3600;
  ASSERT_EQ(true, micro_meta.is_expired(SS_DEF_CACHE_EXPIRATION_TIME_S));
  micro_meta.update_newest_access_time_s();
  micro_meta.update_access_time_s(120);
  ASSERT_EQ(false, micro_meta.is_expired(SS_DEF_CACHE_EXPIRATION_TIME_S));
}

TEST_F(TestSSMicroCacheCommonMeta, mem_block)
{
  ObArenaAllocator allocator;
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockPool mem_blk_pool(cache_stat);

  // 1. init mem_block
  ObSSMemBlock mem_blk(mem_blk_pool);
  const int32_t mem_blk_buf_size = BLOCK_SIZE;
  char *mem_blk_buf = static_cast<char *>(allocator.alloc(mem_blk_buf_size));
  ASSERT_NE(nullptr, mem_blk_buf);
  ASSERT_EQ(OB_SUCCESS, mem_blk.init(OB_SERVER_TENANT_ID, mem_blk_buf_size, mem_blk_buf));
  ASSERT_EQ(1, mem_blk.ref_cnt_);
  ASSERT_EQ(true, mem_blk.is_valid());
  ASSERT_EQ(mem_blk_buf, mem_blk.mem_blk_buf_);
  ASSERT_EQ(mem_blk_buf_size, mem_blk.mem_blk_size_);

  // 2. write micro_block
  const int64_t offset = 100;
  const int64_t size = (1L << 14);
  ObSSMicroBlockId micro_id(MacroBlockId(0, 100, 0), offset, size);
  const ObSSMicroBlockCacheKey micro_key(micro_id);
  char *data_buf = static_cast<char*>(allocator.alloc(size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', size);
  int32_t data_offset = -1;
  int32_t index_offset = -1;
  uint32_t crc = 0;
  ASSERT_EQ(OB_SUCCESS, mem_blk.calc_write_location(micro_key, size, data_offset, index_offset));
  ASSERT_EQ(OB_SUCCESS, mem_blk.write_micro_data(micro_key, data_buf, size, data_offset, index_offset, crc));

  // 3. check each field of mem_block
  ObSSMicroBlockIndex micro_index(micro_key, size);
  ASSERT_EQ(1, mem_blk.micro_cnt_);
  ASSERT_EQ(1, mem_blk.incomplete_cnt_);
  ASSERT_EQ(size, mem_blk.total_data_size_);
  ASSERT_EQ(micro_index.get_serialize_size(), mem_blk.total_index_size_);
  ASSERT_EQ(false, mem_blk.can_flush());
  mem_blk.mark_micro_complete();
  ASSERT_EQ(true, mem_blk.can_flush());

  ObSSMemBlockHandle handle;
  handle.set_ptr(&mem_blk);
  ASSERT_EQ(true, handle.is_valid());
  ASSERT_EQ(2, mem_blk.ref_cnt_);
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, mem_blk.try_free(succ_free));
  ASSERT_EQ(false, succ_free);
  ASSERT_EQ(1, mem_blk.ref_cnt_);
}

TEST_F(TestSSMicroCacheCommonMeta, mem_block_pool)
{
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockPool mem_blk_pool(cache_stat);
  ObArray<ObSSMemBlock*> fg_blk_arr;
  ObArray<ObSSMemBlock*> bg_blk_arr;

  const int64_t max_fg_cnt = 10;
  const int64_t max_bg_cnt = 5;
  mem_blk_pool.init(OB_SERVER_TENANT_ID, BLOCK_SIZE, max_fg_cnt, max_bg_cnt);

  // 1. alloc mem_blk
  ObSSMemBlock *mem_blk = nullptr;
  for (int64_t i = 0; i < max_fg_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk, true/* is_fg */));
    ASSERT_EQ(OB_SUCCESS, fg_blk_arr.push_back(mem_blk));
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_blk_pool.alloc(mem_blk, true/* is_fg */));

  for (int64_t i = 0; i < max_bg_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk, false/* is_fg */));
    ASSERT_EQ(OB_SUCCESS, bg_blk_arr.push_back(mem_blk));
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mem_blk_pool.alloc(mem_blk, false/* is_fg */));

  cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_ = max_fg_cnt;
  cache_stat.mem_blk_stat().mem_blk_bg_used_cnt_ = max_bg_cnt;
  cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_ = max_fg_cnt;
  cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_ = max_bg_cnt;

  // 2. free mem_blk
  bool succ_free = false;
  for (int64_t i = 0; i < fg_blk_arr.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, fg_blk_arr[i]->try_free(succ_free));
    ASSERT_EQ(true, succ_free);
  }
  for (int64_t i = 0; i < bg_blk_arr.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, bg_blk_arr[i]->try_free(succ_free));
    ASSERT_EQ(true, succ_free);
  }
  cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_ = 0;
  cache_stat.mem_blk_stat().mem_blk_bg_used_cnt_ = 0;
}

TEST_F(TestSSMicroCacheCommonMeta, aggregate_micro_info)
{
  struct TmpAggregateMicroInfoCmp
  {
  public:
    bool operator()(const ObSSAggregateMicroInfo &l, const ObSSAggregateMicroInfo &r)
    {
      return l < r;
    }
  };
  #define BUILD_AGGRATE_MICRO_INFO(is_in_l1, tablet_id_val, ls_id_val) \
    aggr_micro_info.is_in_l1_ = is_in_l1; \
    aggr_micro_info.tablet_id_ = ObTabletID(tablet_id_val); \
    aggr_micro_info.ls_id_ = ObLSID(ls_id_val); \
    ASSERT_EQ(OB_SUCCESS, aggr_micro_info_arr.push_back(aggr_micro_info)); \

  #define CHECK_AGGRATE_MICRO_INFO(real_aggr_micro_info, exp_is_in_l1, exp_tablet_id_val, exp_ls_id_val) \
    ASSERT_EQ(real_aggr_micro_info.is_in_l1_, exp_is_in_l1); \
    ASSERT_EQ(real_aggr_micro_info.tablet_id_, ObTabletID(exp_tablet_id_val)); \
    ASSERT_EQ(real_aggr_micro_info.ls_id_, ObLSID(exp_ls_id_val)); \

  ObArray<ObSSAggregateMicroInfo> aggr_micro_info_arr;
  ObSSAggregateMicroInfo aggr_micro_info;
  BUILD_AGGRATE_MICRO_INFO(true, 100, 10);
  BUILD_AGGRATE_MICRO_INFO(true, 101, 10);
  BUILD_AGGRATE_MICRO_INFO(true, 100, 10);
  BUILD_AGGRATE_MICRO_INFO(false, 99, 10);
  BUILD_AGGRATE_MICRO_INFO(false, 101, 10);
  BUILD_AGGRATE_MICRO_INFO(false, 99, 8);
  BUILD_AGGRATE_MICRO_INFO(true, 101, 10);
  BUILD_AGGRATE_MICRO_INFO(true, 99, 10);
  BUILD_AGGRATE_MICRO_INFO(true, 100, 8);
  BUILD_AGGRATE_MICRO_INFO(true, 100, 12);
  BUILD_AGGRATE_MICRO_INFO(false, 99, 12);
  BUILD_AGGRATE_MICRO_INFO(false, 100, 12);
  BUILD_AGGRATE_MICRO_INFO(false, 99, 12);
  BUILD_AGGRATE_MICRO_INFO(false, 101, 10);
  std::sort(&aggr_micro_info_arr.at(0), &aggr_micro_info_arr.at(0) + aggr_micro_info_arr.count(), TmpAggregateMicroInfoCmp());

  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(0), true, 99, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(1), true, 100, 8);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(2), true, 100, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(3), true, 100, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(4), true, 100, 12);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(5), true, 101, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(6), true, 101, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(7), false, 99, 8);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(8), false, 99, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(9), false, 99, 12);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(10), false, 99, 12);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(11), false, 100, 12);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(12), false, 101, 10);
  CHECK_AGGRATE_MICRO_INFO(aggr_micro_info_arr.at(13), false, 101, 10);

  aggr_micro_info_arr.reset();
  const uint64_t total_cnt = 100000;
  for (int64_t i = 0; i < total_cnt; ++i) {
    const bool is_in_l1 = ObRandom::rand(0, 1);
    const uint64_t tablet_id = ObRandom::rand(1, 30);
    const uint64_t ls_id = ObRandom::rand(1, 10);
    BUILD_AGGRATE_MICRO_INFO(is_in_l1, tablet_id, ls_id);
  }
  std::sort(&aggr_micro_info_arr.at(0), &aggr_micro_info_arr.at(0) + aggr_micro_info_arr.count(), TmpAggregateMicroInfoCmp());
  for (int64_t i = 0; i < total_cnt; ++i) {
    const ObSSAggregateMicroInfo &cur_info = aggr_micro_info_arr.at(i);
    ASSERT_LE(1, cur_info.tablet_id_.id());
    ASSERT_GE(30, cur_info.tablet_id_.id());
    ASSERT_LE(1, cur_info.ls_id_.id());
    ASSERT_GE(10, cur_info.ls_id_.id());
  }
}

TEST_F(TestSSMicroCacheCommonMeta, item_compatibility)
{
  const int64_t buf_size = 512;
  char buf[buf_size];
  MEMSET(buf, '\0', buf_size);

  MockSSCkptItemHeader mock_item_header;
  mock_item_header.payload_size_ = 100007;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, mock_item_header.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, mock_item_header.get_serialize_size());

  MockSSCkptItemChildHeader mock_item_child_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, mock_item_child_header.deserialize(buf, buf_size, pos));
  ASSERT_NE(pos, mock_item_child_header.get_serialize_size());
  ASSERT_EQ(mock_item_header.payload_size_, mock_item_child_header.payload_size_);
}

TEST_F(TestSSMicroCacheCommonMeta, local_cache_control_mode)
{
  ObSSLocalCacheControlMode cache_mode;
  ASSERT_EQ(true, cache_mode.is_micro_cache_enable()); // default is true
  ASSERT_EQ(true, cache_mode.is_macro_read_cache_enable());
  ASSERT_EQ(true, cache_mode.is_macro_write_cache_enable());

  cache_mode.set_micro_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  cache_mode.set_macro_read_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  cache_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  ASSERT_EQ(true, cache_mode.is_micro_cache_enable());
  ASSERT_EQ(true, cache_mode.is_macro_read_cache_enable());
  ASSERT_EQ(true, cache_mode.is_macro_write_cache_enable());

  cache_mode.set_micro_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  cache_mode.set_macro_read_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  cache_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  ASSERT_EQ(false, cache_mode.is_micro_cache_enable());
  ASSERT_EQ(false, cache_mode.is_macro_read_cache_enable());
  ASSERT_EQ(false, cache_mode.is_macro_write_cache_enable());
}

TEST_F(TestSSMicroCacheCommonMeta, micro_sub_range_info)
{
  ObSSMicroSubRangeInfo sub_rng_info;
  ASSERT_EQ(false, sub_rng_info.is_valid());
  sub_rng_info.set_end_hval(1);
  sub_rng_info.inc_ref_count();
  ASSERT_EQ(2, sub_rng_info.ref_cnt_);
  sub_rng_info.ref_cnt_ = 10; // increase the ref_cnt_ to prevent destroy_sub_range being excuted
  ASSERT_EQ(true, sub_rng_info.contain_hval(0));
  ObSSMicroSubRangeInfo *range_contain_hval = nullptr;
  sub_rng_info.get_sub_range_by_hval(0, range_contain_hval);
  ASSERT_EQ(&sub_rng_info, range_contain_hval);
  range_contain_hval = nullptr;
  sub_rng_info.get_sub_range_by_hval(5, range_contain_hval);
  ASSERT_EQ(nullptr, range_contain_hval);
  ASSERT_EQ(true, sub_rng_info.is_valid());
  ASSERT_EQ(0, sub_rng_info.get_micro_meta_cnt());
  ASSERT_EQ(nullptr, sub_rng_info.get_first_micro_meta());
  ASSERT_EQ(nullptr, sub_rng_info.get_next_range());
  ASSERT_EQ(true, sub_rng_info.is_empty());

  // link a meta
  ObSSMicroBlockMeta block_meta;
  block_meta.ref_cnt_ = 10; // increase the ref_cnt_ to prevent core when deconstuct
  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(&block_meta);
  ASSERT_EQ(true, micro_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, sub_rng_info.link_micro_meta(micro_handle, 1));
  ASSERT_EQ(true, sub_rng_info.has_micro_meta());

  // link a range
  ObSSMicroSubRangeInfo *rng1 = new ObSSMicroSubRangeInfo();
  ASSERT_NE(nullptr, rng1);
  rng1->set_end_hval(10);
  ASSERT_EQ(true, rng1->is_valid());
  ASSERT_EQ(OB_SUCCESS, sub_rng_info.link_next_range(rng1));
  ObSSMicroSubRangeInfo *target_sub_range = nullptr;
  sub_rng_info.get_sub_range_by_hval(9, target_sub_range);
  ASSERT_EQ(rng1, target_sub_range);
  ObSSMicroSubRangeInfo *rng2 = new ObSSMicroSubRangeInfo();
  ASSERT_NE(nullptr, rng2);
  rng2->set_end_hval(20);
  ASSERT_EQ(true, rng2->is_valid());
  ASSERT_EQ(OB_SUCCESS, rng1->link_next_range(rng2));
  target_sub_range = nullptr;
  sub_rng_info.get_sub_range_by_hval(19, target_sub_range);
  ASSERT_EQ(rng2, target_sub_range);
  sub_rng_info.set_next_range(nullptr);
  ASSERT_EQ(false, sub_rng_info.has_next_range());
  sub_rng_info.next_ = nullptr;
  delete rng1;
  delete rng2;

  sub_rng_info.dec_ref_count();
  ASSERT_EQ(9, sub_rng_info.ref_cnt_);
}

TEST_F(TestSSMicroCacheCommonMeta, init_rng_info)
{
  ObSSMicroInitRangeInfo init_rng_info;
  ASSERT_EQ(false, init_rng_info.has_sub_ranges());
  ASSERT_EQ(0, init_rng_info.get_total_micro_cnt());
  init_rng_info.end_hval_ = 100;

  // init sub range info that will be inserted into init_rng_info
  int64_t total_sub_range_cnt = 10;
  ObSSMicroSubRangeInfo *sub_range_lst = new ObSSMicroSubRangeInfo();
  sub_range_lst->set_end_hval(1);
  ASSERT_EQ(true, sub_range_lst->is_valid());
  ObSSMicroSubRangeInfo *curr_sub_range = sub_range_lst;
  for(int64_t i = 1; i < total_sub_range_cnt; ++i){
    ObSSMicroSubRangeInfo *micro_sub_rng_info_ptr = new ObSSMicroSubRangeInfo();
    micro_sub_rng_info_ptr->set_end_hval(i * 10);
    ASSERT_EQ(true, micro_sub_rng_info_ptr->is_valid());
    curr_sub_range->set_next_range(micro_sub_rng_info_ptr);
    curr_sub_range = curr_sub_range->get_next_range();
  }
  ObSSMicroSubRangeInfo *old_sub_range_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_rng_info.link_sub_ranges(sub_range_lst, old_sub_range_list));
  ASSERT_EQ(true, init_rng_info.has_sub_ranges());
  ASSERT_EQ(total_sub_range_cnt, init_rng_info.get_sub_range_cnt());
  ASSERT_EQ(sub_range_lst, init_rng_info.get_first_sub_range());

  // check sub_ranges can be access by hval
  for(int64_t i = 0; i < total_sub_range_cnt; ++i){
    ObSSMicroSubRangeHandle range_handle;
    init_rng_info.get_sub_range_by_hval(i * 10, range_handle);
    ASSERT_EQ(true, range_handle.is_valid());
    ASSERT_LE(i * 10, range_handle.get_ptr()->end_hval_);
  }

  // insert micro meta
  const int64_t total_meta_cnt = 10;
  ObSSMicroBlockMeta *meta_ptr_arr[total_meta_cnt];
  for(int64_t i = 0; i < total_meta_cnt; ++i){
    ObSSMicroBlockMetaHandle micro_handle;
    ObSSMicroBlockMeta *block_meta_ptr = new ObSSMicroBlockMeta();
    meta_ptr_arr[i] = block_meta_ptr;
    block_meta_ptr->ref_cnt_ = 10; // avoid core when be deconstructed
    micro_handle.set_ptr(block_meta_ptr);
    ASSERT_EQ(true, micro_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS, init_rng_info.link_micro_meta_by_hval(i * 10, micro_handle));
  }
  ASSERT_EQ(total_meta_cnt, init_rng_info.get_total_micro_cnt());

  // release sub range info list
  curr_sub_range = sub_range_lst;
  while(nullptr != curr_sub_range){
    ObSSMicroSubRangeInfo* curr = curr_sub_range;
    curr_sub_range = curr_sub_range->get_next_range();
    delete curr;
  }
  //release meta
  for(int64_t i = 0; i < total_meta_cnt; ++i){
    delete meta_ptr_arr[i];
  }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_common_meta.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_common_meta.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
