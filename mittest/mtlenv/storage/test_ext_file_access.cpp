// owner: jiahua.cjh
// owner group: storage

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

#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE



#define protected public
#define private public

#include "lib/alloc/memory_dump.h"
#include "storage/schema_utils.h"
#include "storage/mock_ob_log_handler.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "sql/engine/table/ob_external_data_access_mgr.h"

// last include
#include "storage/test_dml_common.h"

namespace oceanbase
{
namespace sql
{

ObExternalDataAccessMgr *mgr_ = nullptr;

class TestExtFileAccess : public ::testing::Test
{
public:
  TestExtFileAccess();
  virtual ~TestExtFileAccess() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  // without page_cache
  int check_correct_for_two_arr(
    const ObIArray<ObExtCacheMissSegment> &sg_arr,
    const ObIArray<ObExternalReadInfo> &rd_info_arr);
  // with page_cache
  int check_correct_for_two_arr(
    const int64_t page_cache_size,
    const ObIArray<ObExtCacheMissSegment> &sg_arr,
    const ObIArray<ObExternalReadInfo> &rd_info_arr);
public:
  static const uint64_t TENANT_ID = 1;
  static const share::ObLSID LS_ID;
  common::ObArenaAllocator allocator_;
};

int TestExtFileAccess::check_correct_for_two_arr(
    const ObIArray<ObExtCacheMissSegment> &sg_arr,
    const ObIArray<ObExternalReadInfo> &rd_info_arr)
{
  int ret = OB_SUCCESS;

  if (sg_arr.count() != rd_info_arr.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count dismatch", K(ret), K(sg_arr), K(rd_info_arr));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < sg_arr.count(); i++) {
    const ObExtCacheMissSegment &cur_sg = sg_arr.at(i);
    const ObExternalReadInfo &cur_info = rd_info_arr.at(i);
    if (cur_info.buffer_ != static_cast<void*>(cur_sg.buf_) ||
        cur_info.size_ != cur_sg.get_rd_len() ||
        cur_info.offset_ != cur_sg.get_rd_offset()) {
      ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info dismatch", K(ret), K(cur_sg), K(cur_info));
    }
  }

  return ret;
}

TestExtFileAccess::TestExtFileAccess()
  : allocator_()
{
}

void TestExtFileAccess::SetUp()
{
}

void TestExtFileAccess::TearDown()
{
}

void TestExtFileAccess::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestExtFileAccess::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  MockTenantModuleEnv::get_instance().destroy();
}


TEST_F(TestExtFileAccess, test_buffer_split_without_page_cache)
{
  int ret = OB_SUCCESS;
  mgr_ = MTL(ObExternalDataAccessMgr*);
  if (mgr_ == nullptr) {
    LOG_WARN("mgr is nullptr, abort", K(mgr_));
    ob_abort();
  }

  ObIOFd fd; // mock
  ObExternalFileReadHandle handle;
  ObSArray<ObExtCacheMissSegment> sg_arr;
  ObSArray<ObExternalReadInfo> rd_info_arr;
  ObIOFlag io_desc;
  const int64_t BLOCK_SIZE = 2 * 1024 * 1024;
  const int64_t modify_time = 1024;
  const int64_t page_size = 512 * 1024;
  void *buf = allocator_.alloc(BLOCK_SIZE);
  int64_t cur_offset;
  int64_t cur_size;
  ObString url("/path/to/file");
  ObString content_digest("");

  // init fd and io_desc
  fd.is_valid();
  fd.first_id_ = 100;
  fd.second_id_ = 100;
  io_desc.set_mode(ObIOMode::READ);
  io_desc.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);

  // init req  [100, 2MB+100) => [100, 2MB) [2MB, 2MB+100]
  sg_arr.reset();
  rd_info_arr.reset();
  cur_offset = 100;
  cur_size = BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, mgr_->fill_cache_hit_buf_and_get_cache_miss_segments_(fd,
                                                                              url,
                                                                              content_digest,
                                                                              modify_time,
                                                                              page_size,
                                                                              cur_offset,
                                                                              cur_size,
                                                                              false,
                                                                              static_cast<char*>(buf),
                                                                              handle,
                                                                              sg_arr));
  ASSERT_EQ(0, handle.cache_hit_size_);
  ASSERT_EQ(2, sg_arr.count());
  ASSERT_EQ(cur_offset, sg_arr[0].get_rd_offset());
  ASSERT_EQ(0, sg_arr[0].get_page_offset(page_size));
  ASSERT_EQ(cur_size - cur_offset, sg_arr[0].get_rd_len()); // split by 2MB boundary;

  ASSERT_EQ(cur_size, sg_arr[1].get_rd_offset());
  ASSERT_EQ(cur_size, sg_arr[1].get_page_offset(page_size));
  ASSERT_EQ(cur_offset, sg_arr[1].get_rd_len()); // split by 2MB boundary;

  ObExternalReadInfo info(
      cur_offset,
      buf,
      cur_size,
      INT64_MAX,
      io_desc);
  ASSERT_EQ(OB_SUCCESS, mgr_->get_rd_info_arr_by_cache_miss_seg_arr_(
                          fd, url, content_digest, modify_time, page_size, 0, sg_arr, info, false,
                          rd_info_arr));
  ASSERT_EQ(OB_SUCCESS, check_correct_for_two_arr(sg_arr, rd_info_arr));

  // init req [2048, 3072) => [2048, 3072)
  sg_arr.reset();
  rd_info_arr.reset();
  cur_offset = 2048;
  cur_size = 1024;
  ASSERT_EQ(OB_SUCCESS, mgr_->fill_cache_hit_buf_and_get_cache_miss_segments_(fd,
                                                                              url,
                                                                              content_digest,
                                                                              modify_time,
                                                                              page_size,
                                                                              cur_offset,
                                                                              cur_size,
                                                                              false,
                                                                              static_cast<char*>(buf),
                                                                              handle,
                                                                              sg_arr));
  ASSERT_EQ(0, handle.cache_hit_size_);
  ASSERT_EQ(1, sg_arr.count());
  ASSERT_EQ(cur_offset, sg_arr[0].get_rd_offset());
  ASSERT_EQ(0, sg_arr[0].get_page_offset(page_size));
  ASSERT_EQ(cur_size, sg_arr[0].get_rd_len());

  ObExternalReadInfo info2(
      cur_offset,
      buf,
      cur_size,
      INT64_MAX,
      io_desc);
  ASSERT_EQ(OB_SUCCESS, mgr_->get_rd_info_arr_by_cache_miss_seg_arr_(
                          fd, url, content_digest, modify_time, page_size, 0, sg_arr, info2, false,
                          rd_info_arr));
  ASSERT_EQ(OB_SUCCESS, check_correct_for_two_arr(sg_arr, rd_info_arr));


  // init req [2MB-1, 2MB+1) => [2MB-1, 2MB) [2MB, 2MB+1)
  sg_arr.reset();
  rd_info_arr.reset();
  cur_offset = BLOCK_SIZE-1;
  cur_size = 2;
  ASSERT_EQ(OB_SUCCESS, mgr_->fill_cache_hit_buf_and_get_cache_miss_segments_(fd,
                                                                              url,
                                                                              content_digest,
                                                                              modify_time,
                                                                              page_size,
                                                                              cur_offset,
                                                                              cur_size,
                                                                              false,
                                                                              static_cast<char*>(buf),
                                                                              handle,
                                                                              sg_arr));
  ASSERT_EQ(0, handle.cache_hit_size_);
  ASSERT_EQ(2, sg_arr.count());
  ASSERT_EQ(cur_offset, sg_arr[0].get_rd_offset());
  ASSERT_EQ(1, sg_arr[0].get_rd_len());
  ASSERT_EQ(BLOCK_SIZE, sg_arr[1].get_rd_offset());
  ASSERT_EQ(1, sg_arr[1].get_rd_len());

  ObExternalReadInfo info3(
      cur_offset,
      buf,
      cur_size,
      INT64_MAX,
      io_desc);
  ASSERT_EQ(OB_SUCCESS, mgr_->get_rd_info_arr_by_cache_miss_seg_arr_(
                          fd, url, content_digest, modify_time, page_size, 0, sg_arr, info3, false,
                          rd_info_arr));
  ASSERT_EQ(OB_SUCCESS, check_correct_for_two_arr(sg_arr, rd_info_arr));


  // init req [2MB-1000, 10MB-1000) => [2MB-1000, 2MB) [2MB, 4MB) [4MB, 6MB) [6MB, 8MB) [8MB, 10MB-1000)
  sg_arr.reset();
  rd_info_arr.reset();
  cur_offset = BLOCK_SIZE-1000;
  cur_size = 4 * BLOCK_SIZE;
  ASSERT_EQ(OB_SUCCESS, mgr_->fill_cache_hit_buf_and_get_cache_miss_segments_(fd,
                                                                              url,
                                                                              content_digest,
                                                                              modify_time,
                                                                              page_size,
                                                                              cur_offset,
                                                                              cur_size,
                                                                              false,
                                                                              static_cast<char*>(buf),
                                                                              handle,
                                                                              sg_arr));
  ASSERT_EQ(0, handle.cache_hit_size_);
  ASSERT_EQ(5, sg_arr.count());
  ASSERT_EQ(cur_offset, sg_arr[0].get_rd_offset());
  ASSERT_EQ(1000, sg_arr[0].get_rd_len());
  ASSERT_EQ(BLOCK_SIZE, sg_arr[1].get_rd_offset());
  ASSERT_EQ(BLOCK_SIZE, sg_arr[1].get_rd_len());
  ASSERT_EQ(BLOCK_SIZE * 2, sg_arr[2].get_rd_offset());
  ASSERT_EQ(BLOCK_SIZE, sg_arr[2].get_rd_len());
  ASSERT_EQ(BLOCK_SIZE * 3, sg_arr[3].get_rd_offset());
  ASSERT_EQ(BLOCK_SIZE, sg_arr[3].get_rd_len());
  ASSERT_EQ(BLOCK_SIZE * 4, sg_arr[4].get_rd_offset());
  ASSERT_EQ(BLOCK_SIZE - 1000, sg_arr[4].get_rd_len());

  ObExternalReadInfo info4(
      cur_offset,
      buf,
      cur_size,
      INT64_MAX,
      io_desc);
  ASSERT_EQ(OB_SUCCESS, mgr_->get_rd_info_arr_by_cache_miss_seg_arr_(
                          fd, url, content_digest, modify_time, page_size, 0, sg_arr, info4, false,
                          rd_info_arr));
  ASSERT_EQ(OB_SUCCESS, check_correct_for_two_arr(sg_arr, rd_info_arr));
}

TEST_F(TestExtFileAccess, test_file_map_key) {
#define EXPECT_SUCC(expr) ASSERT_EQ((expr), OB_SUCCESS)
  using FileMapKey = ObExternalDataAccessMgr::FileMapKey;

  ObMemAttr mem_attr(TENANT_ID, "testFileMapKey");
  ObFIFOAllocator allocator(TENANT_ID);

  EXPECT_SUCC(allocator.init(lib::ObMallocAllocator::get_instance(), 4096, mem_attr));

  ObHashMap<FileMapKey, ObExternalAccessFileInfo*> map;

  auto make_obstr = [&](const char *str, ObString &out)->void* {
    size_t size = strlen(str);
    void *buf = allocator.alloc(size);
    if (nullptr == buf) {
      return buf;
    }
    out.assign_buffer((char*)buf, size);
    auto write_size = out.write(str, size);
    if (size != write_size) {
      allocator.free(buf);
      buf = nullptr;
    }
    return buf;
  };

  auto make_file_info = [&](const ObString &str)->ObExternalAccessFileInfo* {
    void *buf = allocator.alloc(sizeof(ObExternalAccessFileInfo));
    ObExternalAccessFileInfo *out = nullptr;
    if (buf == nullptr) {
      return out;
    }
    int ret = OB_SUCCESS;
    int64_t file_size = 0;
    const int64_t modify_time = 1024;
    const int64_t page_size = 512 * 1024;
    ObString content_digest("");
    out = new(buf) ObExternalAccessFileInfo;
    ObObjectStorageInfo access_info;
    access_info.set(OB_STORAGE_HDFS, "dummy");
    if (OB_FAIL(out->set_access_info(&access_info, &allocator))) {
      LOG_WARN("failed to set access info", K(ret));
    } else if (OB_FAIL(out->set_basic_file_info(str, content_digest, modify_time, page_size,
                                                file_size, allocator))) {
      LOG_WARN("failed to set access info", K(ret));
    }
    if (OB_FAIL(ret)) {
      out->~ObExternalAccessFileInfo();
      allocator.free(out);
      out = nullptr;
    }
    return out;
  };

  EXPECT_SUCC(map.create(11, mem_attr));

  {
    ObString str;
    const int64_t modify_time = 1024;
    const int64_t page_size = 512 * 1024;
    ObString content_digest("");
    void *buf = make_obstr("key0", str);
    ASSERT_NE(nullptr, buf);

    FileMapKey key(&allocator);
    EXPECT_SUCC(key.init(str, content_digest, modify_time, page_size));

    ObExternalAccessFileInfo *info = make_file_info(str);
    ASSERT_NE(nullptr, info);
    EXPECT_SUCC(map.set_refactored(key, info));
    memset(buf, 0, str.size());
    allocator.free(buf);
  }

  {
    ObString str("key1");
    const int64_t modify_time = 1024;
    const int64_t page_size = 512 * 1024;
    ObString content_digest("");
    FileMapKey key(&allocator);
    EXPECT_SUCC(key.init(str, content_digest, modify_time, page_size));

    ObExternalAccessFileInfo *info = make_file_info(str);
    ASSERT_NE(nullptr, info);
    EXPECT_SUCC(map.set_refactored(key, info));
  }

  ObExternalAccessFileInfo *info = nullptr;
  FileMapKey key(&allocator);
  const int64_t modify_time = 1024;
  const int64_t page_size = 512 * 1024;
  ObString content_digest("");
  EXPECT_SUCC(key.init(ObString("key0"), content_digest, modify_time, page_size));
  EXPECT_SUCC(map.get_refactored(key, info));
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(ObString("key0") == info->get_url());
  ASSERT_TRUE(info->get_access_info() != nullptr);
  ASSERT_TRUE(info->get_access_info()->is_hdfs_storage());
  ASSERT_TRUE(info->get_access_info()->is_valid());

  key.reset();
  info = nullptr;
  key.modify_time_ = 0;
  key.allocator_ = &allocator;
  EXPECT_SUCC(key.init(ObString("key1"), content_digest, modify_time, page_size));
  EXPECT_SUCC(map.get_refactored(key, info));
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(ObString("key1") == info->get_url());
  ASSERT_TRUE(info->get_access_info() != nullptr);
  ASSERT_TRUE(info->get_access_info()->is_hdfs_storage());
  ASSERT_TRUE(info->get_access_info()->is_valid());

#undef EXPECT_SUCC
}

TEST_F(TestExtFileAccess, test_ext_page_cache_key) {
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  common::ObIKVCacheKey *key = nullptr;
  {
    std::string url = "/path/to/file";
    const int64_t modify_time = 1024;
    const int64_t page_size = 512 * 1024;
    ObExternalDataPageCacheKey tmp(url.data(), url.size(),  nullptr, 0, modify_time, page_size, 0, 789);

    char *buf = (char*)allocator.alloc(tmp.size());
    EXPECT_NE(nullptr, buf);
    EXPECT_TRUE(OB_SUCCESS == tmp.deep_copy(buf, tmp.size(), key));
    EXPECT_EQ((void*)buf, (void*)key);
    memset(url.data(), 0, url.size());
  }
  std::string url = "/path/to/file";
  const int64_t modify_time = 1024;
  const int64_t page_size = 512 * 1024;
  EXPECT_TRUE(key->operator==(ObExternalDataPageCacheKey(url.data(), url.size(), nullptr, 0, modify_time, page_size, 0, 789)));
  LOG_INFO("print ext page cache key", KPC(static_cast<ObExternalDataPageCacheKey*>(key)));

  key->~ObIKVCacheKey();
  allocator.free(key);
}

} // end sql
} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ext_file_access.log*");
  OB_LOGGER.set_file_name("test_ext_file_access.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
