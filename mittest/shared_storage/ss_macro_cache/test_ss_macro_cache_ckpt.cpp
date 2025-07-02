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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

class TestSSMacroCacheCkpt : public ::testing::Test
{
public:
  TestSSMacroCacheCkpt() : info_base_(), enable_test_(false), init_ckpt_verison_(0) {}
  virtual ~TestSSMacroCacheCkpt() {}

  virtual void SetUp() override
  {
    if (ObObjectStorageUnittestCommon::need_skip_test(S3_SK)) {
      enable_test_ = false;
    } else {
      enable_test_ = true;
    }

    if (enable_test_) {
      OK(ObObjectStorageUnittestCommon::set_storage_info(
          S3_BUCKET, S3_ENDPOINT, S3_AK, S3_SK,
          nullptr/*appid_*/, S3_REGION, nullptr/*extension_*/,
          info_base_));

      ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
      ASSERT_NE(nullptr, macro_cache_mgr);
      init_ckpt_verison_ = macro_cache_mgr->ckpt_task_.next_version_;
      OK(clean_macro_cache_mgr());
      ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    }
  }

  int clean_macro_cache_mgr()
  {
    return TestSSMacroCacheMgrUtil::clean_macro_cache_mgr();
  }

  virtual void TearDown() override
  {
    info_base_.reset();
  }

  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    OK(MockTenantModuleEnv::get_instance().init());

    ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    ASSERT_EQ(0, macro_cache_mgr->ckpt_task_.next_version_);
    ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    macro_cache_mgr->flush_task_.is_inited_ = false;
    macro_cache_mgr->evict_task_.is_inited_ = false;
    macro_cache_mgr->expire_task_.is_inited_ = false;
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    MockTenantModuleEnv::get_instance().destroy();
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
  }

public:
  ObObjectStorageInfo info_base_;
  bool enable_test_;
  uint64_t init_ckpt_verison_;
};

static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB

TEST_F(TestSSMacroCacheCkpt, test_basic_ckpt)
{
  if (enable_test_) {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);

    {
      LOG_INFO("**************************** 1. empty checkpoint ****************************");
      ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
      macro_cache_mgr->ckpt_task_.runTimerTask();
      ASSERT_EQ(1 + init_ckpt_verison_, macro_cache_mgr->ckpt_task_.next_version_);
      ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());

      uint64_t next_ckpt_version = UINT64_MAX;
      OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
      ASSERT_EQ(1 + init_ckpt_verison_, next_ckpt_version);
      ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    }

    {
      LOG_INFO("**************************** 1. basic checkpoint ****************************");
      // write ckpt
      // 1. put SHARED_MAJOR_DATA_MACRO to ObSSMacroCacheMgr
      const uint64_t tablet_id = 200;
      const uint64_t data_seq = 15;

      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
      macro_id.set_second_id(tablet_id); // tablet_id
      macro_id.set_third_id(data_seq); // data_seq
      ASSERT_TRUE(macro_id.is_valid());

      ObSSMacroCacheInfo macro_cache_info(
          tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
      ASSERT_TRUE(macro_cache_info.is_valid());

      OK(macro_cache_mgr->put(macro_id, macro_cache_info));
      ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
      ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));

      // 4. trigger ckpt
      macro_cache_mgr->ckpt_task_.runTimerTask();
      ASSERT_EQ(2 + init_ckpt_verison_, macro_cache_mgr->ckpt_task_.next_version_);

      // replay ckpt
      // 1. clear cache
      OK(macro_cache_mgr->erase(macro_id));
      ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());

      // 2. replay
      uint64_t next_ckpt_version = UINT64_MAX;
      OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
      ASSERT_EQ(2 + init_ckpt_verison_, next_ckpt_version);
      ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
      ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));

      // 3. errsim macro cache replay ckpt error
      TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MACRO_CACHE_REPLAY_CKPT_ERR, OB_ERR_UNEXPECTED, 0, 1);
      OK(clean_macro_cache_mgr());
      macro_cache_mgr->replay_task_.runTimerTask();
      ASSERT_EQ(2 + init_ckpt_verison_, macro_cache_mgr->ckpt_task_.next_version_);
      ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
      ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
      TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_MACRO_CACHE_REPLAY_CKPT_ERR, OB_ERR_UNEXPECTED, 0, 0);
    }
  }
}

int generate_random_macro_id_arr(MacroBlockId *macro_id_arr, const int64_t n)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_id_arr) || OB_UNLIKELY(n <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(macro_id_arr), K(n));
  } else {
    const uint64_t tablet_id = 200;
    for (int64_t i = 0; OB_SUCC(ret) && i < n; i++) {
      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
      macro_id.set_second_id(tablet_id); // tablet_id
      macro_id.set_third_id(i); // data_seq

      if (OB_UNLIKELY(!macro_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to generate valid macro id", KR(ret), K(i), K(macro_id), K(n));
      } else {
        macro_id_arr[i] = macro_id;
      }
    }
  }
  return ret;
}

int generate_random_macro_cache_meta(
    ObIAllocator &allocator,
    ObSSMacroCacheLRUList::DListType &dlist,
    const bool is_in_fifo_list,
    const int64_t meta_num,
    int64_t &total_serialize_size)
{
  int ret = OB_SUCCESS;
  total_serialize_size = 0;
  MacroBlockId macro_id_arr[meta_num];
  ObSSMacroCacheInfo macro_cache_info(
      200, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  if (meta_num == 0) {
    // do nothing
  } else if (OB_UNLIKELY(meta_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(meta_num));
  } else if (OB_UNLIKELY(!macro_cache_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro cache", KR(ret), K(meta_num), K(macro_cache_info));
  } else if (OB_FAIL(generate_random_macro_id_arr(macro_id_arr, meta_num))) {
    LOG_WARN("fail to generate_random_macro_id_arr", KR(ret), K(meta_num));
  }

  ObSSMacroCacheMeta *meta = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_num; i++) {
    meta = nullptr;
    if (OB_ISNULL(meta = static_cast<ObSSMacroCacheMeta *>(allocator.alloc(sizeof(ObSSMacroCacheMeta))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for macro cache meta", KR(ret),
          K(i), K(macro_id_arr[i]), K(macro_cache_info));
    } else {
      meta = new (meta) ObSSMacroCacheMeta(macro_id_arr[i], macro_cache_info);
      meta->set_is_in_fifo_list(is_in_fifo_list);
      if (OB_UNLIKELY(!meta->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid macro cache meta", KR(ret),
            K(meta_num), K(i), KPC(meta), K(macro_cache_info));
      } else if (OB_UNLIKELY(!dlist.add_first(meta))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add macro cache meta", KR(ret),
            K(meta_num), K(i), KPC(meta), K(macro_cache_info));
      } else {
        total_serialize_size += meta->get_serialize_size();
      }
    }
  }
  LOG_INFO("finish generate_random_macro_cache_meta",
      KR(ret), K(is_in_fifo_list), K(meta_num), K(total_serialize_size));
  return ret;
}

int get_serialized_size_in_buf(const char *buf, const int64_t buf_size, int64_t &serialized_size)
{
  int ret = OB_SUCCESS;
  serialized_size = 0;
  ObSSMacroCacheCkptBLKHeader header;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ckpt block header", KR(ret), KP(buf), K(buf_size));
  } else if (OB_UNLIKELY(header.get_serialize_size() != pos || !header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos unexpected", KR(ret), KP(buf), K(buf_size), K(header));
  } else {
    serialized_size = header.get_payload_size();
  }
  return ret;
}

int test_macro_cache_serialize(
    const int64_t fifo_num,
    const int64_t lru_num,
    const std::function<int64_t(int64_t, int64_t)> &calc_buf_size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMacroCache macro_cache;
  ObSSMacroCacheMeta marker;
  int64_t fifo_serialize_size = 0;
  int64_t lru_serialize_size = 0;

  if (OB_UNLIKELY(fifo_num < 0 || lru_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(fifo_num), K(lru_num));
  } else if (OB_FAIL(ObSSMacroCacheMeta::construct_ckpt_marker(marker))) {
    LOG_WARN("fail to construct_ckpt_marker", KR(ret), K(fifo_num), K(lru_num));
  } else if (OB_FAIL(generate_random_macro_cache_meta(allocator,
      macro_cache.lru_list_.fifo_list_, true/*is_in_fifo_list*/, fifo_num, fifo_serialize_size))) {
    LOG_WARN("fail to construct fifo list", KR(ret), K(fifo_num), K(lru_num));
  } else if (OB_FAIL(generate_random_macro_cache_meta(allocator,
      macro_cache.lru_list_.lru_list_, false/*is_in_fifo_list*/, lru_num, lru_serialize_size))) {
    LOG_WARN("fail to construct lru list", KR(ret), K(fifo_num), K(lru_num));
  } else {
    char *buf = nullptr;
    const int64_t HEADER_FIXED_SERIALIZE_SIZE =
        ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size();
    const int64_t buf_size = HEADER_FIXED_SERIALIZE_SIZE
                           + calc_buf_size(fifo_serialize_size, lru_serialize_size);
    if (OB_UNLIKELY(buf_size <= HEADER_FIXED_SERIALIZE_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to construct valid buf size", KR(ret), K(buf_size), K(fifo_num), K(lru_num),
          K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret), K(buf_size), K(fifo_num), K(lru_num),
          K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE));
    }

    bool is_finished = false;
    int64_t item_cnt = 0;
    int64_t cur_serialized_size = 0;
    while (OB_SUCC(ret) && !is_finished) {
      const int64_t remained_size = fifo_serialize_size + lru_serialize_size - cur_serialized_size;
      int64_t tmp_serialized_size = 0;
      if (OB_FAIL(macro_cache.incremental_serialize(
          buf, buf_size, marker, is_finished, item_cnt))) {
        LOG_WARN("fail to incremental_serialize", KR(ret), K(buf_size), K(fifo_num), K(lru_num),
            K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE));
      } else if ((item_cnt > 0) && OB_FAIL(get_serialized_size_in_buf(buf, buf_size, tmp_serialized_size))) {
        LOG_WARN("fail to get_serialized_size_in_buf", KR(ret), K(buf_size), K(item_cnt),
            K(fifo_num), K(lru_num), K(is_finished), K(HEADER_FIXED_SERIALIZE_SIZE),
            K(fifo_serialize_size), K(lru_serialize_size));
      } else if (OB_UNLIKELY((0 == item_cnt) && !is_finished)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is empty but not finished", KR(ret), K(buf_size), K(tmp_serialized_size),
            K(fifo_num), K(lru_num), KP(buf), K(cur_serialized_size),
            K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
            K(item_cnt), K(is_finished));
      } else if (OB_UNLIKELY((0 == item_cnt) != (0 == remained_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is_empty flag should be equal to (0 == remained_size)", KR(ret), K(buf_size),
            K(fifo_num), K(lru_num), KP(buf),
            K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
            K(tmp_serialized_size), K(cur_serialized_size), K(remained_size),
            K(item_cnt), K(is_finished));
      } else if (OB_UNLIKELY(remained_size < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("remained_size < 0", KR(ret), K(buf_size),
            K(fifo_num), K(lru_num), KP(buf),
            K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
            K(tmp_serialized_size), K(cur_serialized_size), K(remained_size),
            K(item_cnt), K(is_finished));
      } else if (OB_UNLIKELY(cur_serialized_size > fifo_serialize_size && marker.get_is_in_fifo_list())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fifo has been serialized but marker flag invalid", KR(ret), K(buf_size),
            K(fifo_num), K(lru_num), KP(buf),
            K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
            K(tmp_serialized_size), K(cur_serialized_size), K(remained_size),
            K(item_cnt), K(is_finished));
      } else {
        cur_serialized_size += tmp_serialized_size;
      }
    }
    if (FAILEDx(macro_cache.clean_marker())) {
      LOG_INFO("fail to clean marker", K(buf_size), K(fifo_num), K(lru_num), KP(buf),
          K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
          K(cur_serialized_size), K(item_cnt), K(is_finished));
    } else if (OB_UNLIKELY(fifo_num != macro_cache.lru_list_.fifo_list_.get_size()
        || lru_num != macro_cache.lru_list_.lru_list_.get_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_INFO("fail to check list size", K(buf_size), K(fifo_num), K(lru_num), KP(buf),
          K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
          K(cur_serialized_size), K(item_cnt), K(is_finished),
          K(macro_cache.lru_list_.fifo_list_.get_size()), K(macro_cache.lru_list_.lru_list_.get_size()));
    }

    LOG_INFO("finish test_macro_cache_serialize", K(buf_size), K(fifo_num), K(lru_num), KP(buf),
        K(fifo_serialize_size), K(lru_serialize_size), K(HEADER_FIXED_SERIALIZE_SIZE),
        K(cur_serialized_size), K(item_cnt), K(is_finished));
  }
  return ret;
}

TEST_F(TestSSMacroCacheCkpt, test_serialization)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    const int64_t HEADER_FIXED_SERIALIZE_SIZE =
        ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size();

    {
      LOG_INFO("**************************** 1. invalid input ****************************");
      ObSSMacroCache macro_cache;
      char test_buf[HEADER_FIXED_SERIALIZE_SIZE];
      bool is_finished = false;
      int64_t item_cnt = 0;

      // NULL buf
      ObSSMacroCacheMeta marker;
      ASSERT_EQ(OB_INVALID_ARGUMENT, macro_cache.incremental_serialize(
          nullptr, HEADER_FIXED_SERIALIZE_SIZE + 1, marker, is_finished, item_cnt));
      // buf size < HEADER_FIXED_SERIALIZE_SIZE
      ASSERT_EQ(OB_INVALID_ARGUMENT, macro_cache.incremental_serialize(
          test_buf, HEADER_FIXED_SERIALIZE_SIZE - 1, marker, is_finished, item_cnt));
      // buf size = HEADER_FIXED_SERIALIZE_SIZE
      ASSERT_EQ(OB_INVALID_ARGUMENT, macro_cache.incremental_serialize(
          test_buf, HEADER_FIXED_SERIALIZE_SIZE, marker, is_finished, item_cnt));

      // not ckpt marker
      const uint64_t tablet_id = 200;
      const uint64_t data_seq = 15;
      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
      macro_id.set_second_id(tablet_id); // tablet_id
      macro_id.set_third_id(data_seq); // data_seq
      ObSSMacroCacheInfo macro_cache_info(
          tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
      ObSSMacroCacheMeta meta(macro_id, macro_cache_info);
      LOG_INFO("macro cache meta info", K(macro_id), K(macro_cache_info), K(meta));

      ASSERT_TRUE(macro_id.is_valid());
      ASSERT_TRUE(macro_cache_info.is_valid());
      ASSERT_TRUE(meta.is_valid());
      ASSERT_FALSE(meta.is_ckpt_marker());
      ASSERT_EQ(OB_INVALID_ARGUMENT, macro_cache.incremental_serialize(
          test_buf, HEADER_FIXED_SERIALIZE_SIZE, meta, is_finished, item_cnt));
    }

    {
      LOG_INFO("**************************** 2. different data size and buf_size ratio ****************************");
      {
        LOG_INFO("**************************** 2.1 empty fifo and empty lru ****************************");
        OK(test_macro_cache_serialize(
            0/*fifo_num*/, 0/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return 1; }));
      }
      {
        LOG_INFO("**************************** 2.2 buf_size = fifo size + 1 and empty lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 0/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + 1; }));
      }
      {
        LOG_INFO("**************************** 2.3 buf_size = fifo size and empty lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 0/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size; }));
      }
      {
        LOG_INFO("**************************** 2.4 buf_size = fifo size - 1 and empty lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 0/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size - 1; }));
      }
      {
        LOG_INFO("**************************** 2.5 buf_size << fifo size and empty lru ****************************");
        OK(test_macro_cache_serialize(
            5000/*fifo_num*/, 0/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size / 5; }));
      }

      {
        LOG_INFO("**************************** 2.6 empty fifo and buf_size = lru size + 1 ****************************");
        OK(test_macro_cache_serialize(
            0/*fifo_num*/, 1000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return lru_serialize_size + 1; }));
      }
      {
        LOG_INFO("**************************** 2.8 empty fifo and buf_size = lru size ****************************");
        OK(test_macro_cache_serialize(
            0/*fifo_num*/, 1000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return lru_serialize_size; }));
      }
      {
        LOG_INFO("**************************** 2.9 empty fifo and buf_size = lru size - 1 ****************************");
        OK(test_macro_cache_serialize(
            0/*fifo_num*/, 1000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return lru_serialize_size - 1; }));
      }
      {
        LOG_INFO("**************************** 2.10 empty fifo and buf_size << lru size ****************************");
        OK(test_macro_cache_serialize(
            0/*fifo_num*/, 5000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return lru_serialize_size / 5; }));
      }

      {
        LOG_INFO("**************************** 2.11 buf_size >> fifo + lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 500/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return (fifo_serialize_size + lru_serialize_size) * 11; }));
      }
      {
        LOG_INFO("**************************** 2.12 buf_size = fifo + lru - 1 ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 500/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + lru_serialize_size - 1; }));
      }
      {
        LOG_INFO("**************************** 2.13 buf_size = fifo + lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 500/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + lru_serialize_size; }));
      }
      {
        LOG_INFO("**************************** 2.14 buf_size = fifo + lru + 1 ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 500/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + lru_serialize_size + 1; }));
      }
      {
        LOG_INFO("**************************** 2.15 buf_size << fifo + lru ****************************");
        OK(test_macro_cache_serialize(
            5000/*fifo_num*/, 6000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return (fifo_serialize_size + lru_serialize_size) / 11; }));
      }

      {
        LOG_INFO("**************************** 2.16 buf_size = fifo size, lru size > 0 ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 1100/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size; }));

        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 100/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size; }));

        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 1000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size; }));
      }

      {
        LOG_INFO("**************************** 2.17 buf_size > fifo size && buf_size < fifo + lru ****************************");
        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 1100/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + 100; }));

        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 100/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + 100; }));

        OK(test_macro_cache_serialize(
            1000/*fifo_num*/, 1000/*lru_num*/,
            [](int64_t fifo_serialize_size, int64_t lru_serialize_size) { return fifo_serialize_size + 100; }));
      }
    }

    {
      LOG_INFO("**************************** 3. concurrent test ****************************");
      ObArenaAllocator allocator;
      ObSSMacroCache macro_cache;
      const int64_t fifo_num = 2000;
      const int64_t lru_num = 2000;
      int64_t fifo_serialize_size = 0;
      int64_t lru_serialize_size = 0;

      OK(generate_random_macro_cache_meta(
          allocator, macro_cache.lru_list_.fifo_list_,
          true/*is_in_fifo_list*/, fifo_num, fifo_serialize_size));
      OK(generate_random_macro_cache_meta(
          allocator, macro_cache.lru_list_.lru_list_,
          false/*is_in_fifo_list*/, lru_num, lru_serialize_size));
      ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
      ASSERT_EQ(lru_num, macro_cache.lru_list_.lru_list_.get_size());

      {
        LOG_INFO("**************************** 3.1 marker in middle, delete/add next/prev ****************************");
        const int64_t buf_size = HEADER_FIXED_SERIALIZE_SIZE + fifo_serialize_size / 9;
        char buf[buf_size];
        bool is_finished = false;
        int64_t item_cnt = 0;
        ObSSMacroCacheMeta marker;
        OK(ObSSMacroCacheMeta::construct_ckpt_marker(marker));
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());

        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num + 1, macro_cache.lru_list_.fifo_list_.get_size());

        // delete next
        ObSSMacroCacheMeta *node = marker.get_next();
        ASSERT_NE(nullptr, node);
        OK(macro_cache.remove_from_lru_list(*node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());
        // because node was deleted
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());

        // add next
        ASSERT_TRUE(macro_cache.lru_list_.fifo_list_.add_before(marker.get_next(), node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num + 1, macro_cache.lru_list_.fifo_list_.get_size());

        // delete prev
        node = marker.get_prev();
        ASSERT_NE(nullptr, node);
        OK(macro_cache.remove_from_lru_list(*node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());

        // add prev
        ASSERT_TRUE(macro_cache.lru_list_.fifo_list_.add_before(&marker, node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num + 1, macro_cache.lru_list_.fifo_list_.get_size());

        OK(macro_cache.clean_marker());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
      }

      {
        LOG_INFO("**************************** 3.2 marker.get_prev = fifo.get_header(), add prev ****************************");
        const int64_t buf_size = HEADER_FIXED_SERIALIZE_SIZE + fifo_serialize_size;
        char buf[buf_size];
        bool is_finished = false;
        int64_t item_cnt = 0;
        ObSSMacroCacheMeta marker;
        OK(ObSSMacroCacheMeta::construct_ckpt_marker(marker));
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());

        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_TRUE(marker.get_is_in_fifo_list());

        // check fifo finished
        ASSERT_EQ(marker.get_prev(), macro_cache.lru_list_.fifo_list_.get_header());
        ASSERT_EQ(fifo_num + 1, macro_cache.lru_list_.fifo_list_.get_size());

        // add prev
        ObSSMacroCacheMeta *node = macro_cache.lru_list_.fifo_list_.remove_last();
        ASSERT_NE(nullptr, node);
        ASSERT_TRUE(macro_cache.lru_list_.fifo_list_.add_before(&marker, node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_FALSE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num + 1, macro_cache.lru_list_.lru_list_.get_size());

        OK(macro_cache.clean_marker());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num, macro_cache.lru_list_.lru_list_.get_size());
      }

      {
        LOG_INFO("**************************** 3.3 marker.get_prev = lru.get_header(), add prev ****************************");
        const int64_t buf_size = HEADER_FIXED_SERIALIZE_SIZE + fifo_serialize_size + lru_serialize_size;
        char buf[buf_size];
        bool is_finished = false;
        int64_t item_cnt = 0;
        ObSSMacroCacheMeta marker;
        OK(ObSSMacroCacheMeta::construct_ckpt_marker(marker));
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num, macro_cache.lru_list_.lru_list_.get_size());

        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_FALSE(is_finished);
        ASSERT_FALSE(marker.get_is_in_fifo_list());

        // check fifo + lru finished
        ASSERT_EQ(marker.get_prev(), macro_cache.lru_list_.lru_list_.get_header());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num + 1, macro_cache.lru_list_.lru_list_.get_size());

        // add prev
        ObSSMacroCacheMeta *node = macro_cache.lru_list_.lru_list_.remove_last();
        ASSERT_NE(nullptr, node);
        ASSERT_TRUE(macro_cache.lru_list_.lru_list_.add_before(&marker, node));
        OK(macro_cache.incremental_serialize(buf, buf_size, marker, is_finished, item_cnt));
        ASSERT_GT(item_cnt, 0);
        ASSERT_TRUE(is_finished);
        ASSERT_FALSE(marker.get_is_in_fifo_list());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num + 1, macro_cache.lru_list_.lru_list_.get_size());

        OK(macro_cache.clean_marker());
        ASSERT_EQ(fifo_num, macro_cache.lru_list_.fifo_list_.get_size());
        ASSERT_EQ(lru_num, macro_cache.lru_list_.lru_list_.get_size());
      }
    }
  }
}

void print_meta_map(SSMacroCacheMetaMap &map)
{
  int ret = OB_SUCCESS;
  LOG_INFO("print_meta_map MetaMap size: ", K(map.size()));
  for (SSMacroCacheMetaMap::iterator it = map.begin(); it != map.end(); ++it) {
    MacroBlockId macro_id = it->first;
    LOG_INFO("MetaMap Entry: ", K(macro_id), K(it->second), KPC(it->second()));
  }
  LOG_INFO("print_meta_map finish", K(map.size()));
}

TEST_F(TestSSMacroCacheCkpt, test_replay)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    const int64_t HEADER_FIXED_SERIALIZE_SIZE =
        ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size();
    const int64_t DEFAULT_BLK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();

    // Assume the average size of ObSSMacroCacheMeta after serialization is 50B
    const int64_t MACRO_CACHE_META_NUM = 100LL * 1000LL;
    MacroBlockId macro_id_arr[MACRO_CACHE_META_NUM];
    OK(generate_random_macro_id_arr(macro_id_arr, MACRO_CACHE_META_NUM));

    ObSSMacroCacheInfo macro_cache_info(
        200, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
    ASSERT_TRUE(macro_cache_info.is_valid());

    OK(ret);
    int64_t macro_block_type_fifo_list_serialize = 0;
    const int64_t update_ratio = 30;  // 30% in lru list, 70% in fifo list
    ObSSMacroCacheMetaHandle meta_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < MACRO_CACHE_META_NUM; i++) {
      meta_handle.reset();
      macro_cache_info.cache_type_ = static_cast<ObSSMacroCacheType>(ObRandom::rand(
          static_cast<int64_t>(ObSSMacroCacheType::META_FILE),
          static_cast<int64_t>(ObSSMacroCacheType::MAX_TYPE) - 1));
      const bool need_update = ObRandom::rand(1, 100) < update_ratio;
      if (OB_UNLIKELY(!macro_cache_info.is_valid())) {
        LOG_WARN("invalid macro_cache_info", KR(ret), K(i), K(macro_id_arr[i]),
            K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()), K(macro_cache_info));
      } else if (OB_FAIL(macro_cache_mgr->put(macro_id_arr[i], macro_cache_info))) {
        LOG_WARN("fail to put macro meta", KR(ret), K(i), K(macro_id_arr[i]),
            K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()));
      } else if (OB_FAIL(macro_cache_mgr->meta_map_.get_refactored(macro_id_arr[i], meta_handle))) {
        LOG_WARN("fail to get macro meta", KR(ret), K(i), K(macro_id_arr[i]),
            K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()), K(macro_cache_info));
      } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
        LOG_WARN("fail to get macro meta handle", KR(ret), K(i), K(macro_id_arr[i]),
            K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()), K(macro_cache_info));
      } else if (need_update) {
        if (OB_FAIL(macro_cache_mgr->update_lru_list(*(meta_handle())))) {
          LOG_WARN("fail to update_lru_list", KR(ret), K(i), K(macro_id_arr[i]),
              K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()), K(macro_cache_info));
        }
      }

      if (OB_SUCC(ret) && !need_update
          && ObSSMacroCacheType::MACRO_BLOCK == macro_cache_info.cache_type_) {
        macro_block_type_fifo_list_serialize += meta_handle()->get_serialize_size();
      }
    }
    OK(ret);
    ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());

    {
      LOG_INFO("**************************** 1. different data size and buf_size ratio  ****************************");
      macro_cache_mgr->ckpt_task_.parallelism_ = 32;
      {
        LOG_INFO("**************************** 1.1 buf_size << serialize  ****************************");
        macro_cache_mgr->ckpt_task_.ckpt_data_blk_size_ =
            HEADER_FIXED_SERIALIZE_SIZE + macro_block_type_fifo_list_serialize / 3;

        macro_cache_mgr->ckpt_task_.runTimerTask();
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());

        OK(clean_macro_cache_mgr());
        ASSERT_EQ(0, macro_cache_mgr->meta_map_.size());

        uint64_t next_ckpt_version = UINT64_MAX;
        OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
      }
      {
        LOG_INFO("**************************** 1.2 buf_size = serialize  ****************************");
        macro_cache_mgr->ckpt_task_.ckpt_data_blk_size_ =
            HEADER_FIXED_SERIALIZE_SIZE + macro_block_type_fifo_list_serialize;

        macro_cache_mgr->ckpt_task_.runTimerTask();
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());

        OK(clean_macro_cache_mgr());
        ASSERT_EQ(0, macro_cache_mgr->meta_map_.size());

        uint64_t next_ckpt_version = UINT64_MAX;
        OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
      }
      {
        LOG_INFO("**************************** 1.2 buf_size > serialize  ****************************");
        macro_cache_mgr->ckpt_task_.ckpt_data_blk_size_ =
            HEADER_FIXED_SERIALIZE_SIZE + macro_block_type_fifo_list_serialize + 200;

        macro_cache_mgr->ckpt_task_.runTimerTask();
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());

        OK(clean_macro_cache_mgr());
        ASSERT_EQ(0, macro_cache_mgr->meta_map_.size());

        uint64_t next_ckpt_version = UINT64_MAX;
        OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
      }
      {
        LOG_INFO("**************************** 1.2 buf_size >> serialize  ****************************");
        macro_cache_mgr->ckpt_task_.ckpt_data_blk_size_ =
            HEADER_FIXED_SERIALIZE_SIZE + macro_block_type_fifo_list_serialize * 5;

        macro_cache_mgr->ckpt_task_.runTimerTask();
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());

        OK(clean_macro_cache_mgr());
        ASSERT_EQ(0, macro_cache_mgr->meta_map_.size());

        uint64_t next_ckpt_version = UINT64_MAX;
        OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
        ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
      }
    }

    {
      LOG_INFO("**************************** 2. concurrent test  ****************************");
      macro_cache_mgr->ckpt_task_.ckpt_data_blk_size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      std::thread update_thread([&]() {
        int ret = OB_SUCCESS;
        const int64_t start_time_us = ObTimeUtility::fast_current_time();
        const int64_t end_time_us = start_time_us + 5LL * 1000LL * 1000LL; // 5s
        while (ObTimeUtility::fast_current_time() < end_time_us) {
          const int64_t macro_id_idx = ObRandom::rand(0, MACRO_CACHE_META_NUM - 1);
          const MacroBlockId macro_id = macro_id_arr[macro_id_idx];

          ObSSMacroCacheMetaHandle meta_handle;
          if (OB_FAIL(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle))) {
            LOG_WARN("fail to get macro meta", KR(ret), K(macro_id),
                K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()));
          } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
            LOG_WARN("fail to get macro meta handle", KR(ret), K(macro_id),
                K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()));
          } else if (OB_FAIL(macro_cache_mgr->update_lru_list(*(meta_handle())))) {
            LOG_WARN("fail to update_lru_list", KR(ret), K(macro_id),
                K(MACRO_CACHE_META_NUM), K(macro_cache_mgr->meta_map_.size()));
          }

          ret = OB_SUCCESS; // ignore ret
        }
      });

      ob_usleep(100LL * 1000LL); // 100ms
      macro_cache_mgr->ckpt_task_.runTimerTask();
      update_thread.join();
      ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
      print_meta_map(macro_cache_mgr->meta_map_);

      OK(clean_macro_cache_mgr());
      ASSERT_EQ(0, macro_cache_mgr->meta_map_.size());

      uint64_t next_ckpt_version = UINT64_MAX;
      OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
      print_meta_map(macro_cache_mgr->meta_map_);
      ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
    }
  }
}

TEST_F(TestSSMacroCacheCkpt, test_exists_in_both_fifo_and_lru)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    const int64_t HEADER_FIXED_SERIALIZE_SIZE =
        ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size();
    const int64_t DEFAULT_BLK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();

    // Assume the average size of ObSSMacroCacheMeta after serialization is 50B
    const int64_t MACRO_CACHE_META_NUM = 100LL * 1000LL;

    ObArenaAllocator allocator;
    ObSSMacroCache macro_cache;
    const int64_t fifo_num = MACRO_CACHE_META_NUM;
    const int64_t lru_num = MACRO_CACHE_META_NUM;
    int64_t fifo_serialize_size = 0;
    int64_t lru_serialize_size = 0;
    ObSSMacroCacheLRUList &lru_list =
        macro_cache_mgr->macro_caches_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)]->lru_list_;
    lru_list.fifo_list_.clear();
    lru_list.lru_list_.clear();

    OK(generate_random_macro_cache_meta(
        allocator, lru_list.fifo_list_,
        true/*is_in_fifo_list*/, fifo_num, fifo_serialize_size));
    OK(generate_random_macro_cache_meta(
        allocator, lru_list.lru_list_,
        false/*is_in_fifo_list*/, lru_num, lru_serialize_size));
    ASSERT_EQ(fifo_num, lru_list.fifo_list_.get_size());
    ASSERT_EQ(lru_num, lru_list.lru_list_.get_size());
    ASSERT_TRUE(fifo_serialize_size > DEFAULT_BLK_SIZE);
    ASSERT_TRUE(lru_serialize_size > DEFAULT_BLK_SIZE);

    // copy fifo list entry to lru list
    ObSSMacroCacheMeta *fifo_node = lru_list.fifo_list_.get_last();
    ASSERT_NE(nullptr, fifo_node);
    ObSSMacroCacheMeta *lru_node = static_cast<ObSSMacroCacheMeta *>(allocator.alloc(sizeof(ObSSMacroCacheMeta)));
    ASSERT_NE(nullptr, lru_node);
    lru_node = new (lru_node) ObSSMacroCacheMeta();

    lru_node->set_macro_id(fifo_node->get_macro_id());
    lru_node->set_effective_tablet_id(fifo_node->get_effective_tablet_id());
    lru_node->set_size(fifo_node->get_size());
    lru_node->set_last_access_time_us(fifo_node->get_last_access_time_us());
    lru_node->set_cache_type(fifo_node->get_cache_type());
    lru_node->set_is_write_cache(fifo_node->get_is_write_cache());
    lru_node->set_is_in_fifo_list(false);
    ASSERT_TRUE(lru_node->is_valid());
    ASSERT_TRUE(lru_list.lru_list_.add_first(lru_node));

    macro_cache_mgr->ckpt_task_.runTimerTask();
    uint64_t next_ckpt_version = UINT64_MAX;
    OK(macro_cache_mgr->replay_ckpt_(next_ckpt_version));
    ASSERT_EQ(MACRO_CACHE_META_NUM, macro_cache_mgr->meta_map_.size());
    lru_list.fifo_list_.clear();
    lru_list.lru_list_.clear();
  }
}

TEST_F(TestSSMacroCacheCkpt, test_exists_in_lru)
{
  if (enable_test_) {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    const int64_t HEADER_FIXED_SERIALIZE_SIZE =
        ObSSMacroCacheCkptBLKHeader::get_fixed_serialize_size();
    const int64_t DEFAULT_BLK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();

    ObArenaAllocator allocator;
    ObSSMacroCache macro_cache;
    const int64_t fifo_num = 1;
    int64_t fifo_serialize_size = 0;
    ObSSMacroCacheLRUList &lru_list =
        macro_cache_mgr->macro_caches_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)]->lru_list_;
    lru_list.fifo_list_.clear();
    lru_list.lru_list_.clear();

    OK(generate_random_macro_cache_meta(
        allocator, lru_list.fifo_list_,
        true/*is_in_fifo_list*/, fifo_num, fifo_serialize_size));
    ASSERT_EQ(fifo_num, lru_list.fifo_list_.get_size());
    ASSERT_EQ(0, lru_list.lru_list_.get_size());

    // copy fifo list entry to lru list
    ObSSMacroCacheMeta *fifo_node = lru_list.fifo_list_.get_last();
    ASSERT_NE(nullptr, fifo_node);
    ObSSMacroCacheMeta *copy_fifo_node = static_cast<ObSSMacroCacheMeta *>(macro_cache_mgr->macro_meta_allocator_.alloc());
    ASSERT_NE(nullptr, copy_fifo_node);
    copy_fifo_node = new (copy_fifo_node) ObSSMacroCacheMeta();

    copy_fifo_node->set_macro_id(fifo_node->get_macro_id());
    copy_fifo_node->set_effective_tablet_id(fifo_node->get_effective_tablet_id());
    copy_fifo_node->set_size(fifo_node->get_size());
    copy_fifo_node->set_last_access_time_us(fifo_node->get_last_access_time_us());
    copy_fifo_node->set_cache_type(fifo_node->get_cache_type());
    copy_fifo_node->set_is_write_cache(fifo_node->get_is_write_cache());
    copy_fifo_node->set_is_in_fifo_list(fifo_node->get_is_in_fifo_list());
    ASSERT_TRUE(copy_fifo_node->is_valid());
    ASSERT_TRUE(lru_list.fifo_list_.add_first(copy_fifo_node));

    macro_cache_mgr->ckpt_task_.runTimerTask();
    uint64_t next_ckpt_version = UINT64_MAX;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->replay_ckpt_(next_ckpt_version));
    lru_list.fifo_list_.clear();
    lru_list.lru_list_.clear();
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_ckpt.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_ckpt.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}