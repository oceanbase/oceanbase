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
#include "mittest/shared_storage/clean_residual_data.h"
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#undef private
#undef protected
#include "test_pcached_external_file_service.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

const bool is_shared_storage_mode_test = false;

class TestSNPCachedExternalFileService : public TestPCachedExternalFileService
{
public:
  TestSNPCachedExternalFileService() : TestPCachedExternalFileService() {}
  virtual ~TestSNPCachedExternalFileService() {}
};

TEST_F(TestSNPCachedExternalFileService, test_lru_basic)
{
  if (enable_test_) {
    ObPCachedExtLRU lru;
    OK(lru.init(MTL_ID()));

    ObStorageObjectHandle object_handle;
    std::vector<MacroBlockId> macro_ids;
    const int64_t MACRO_NUM = 10;
    const int64_t DATA_SIZE = MB; // 1MB
    for (int64_t i = 0; i < MACRO_NUM; i++) {
      object_handle.reset();
      OK(OB_SERVER_BLOCK_MGR.alloc_object(object_handle));
      const MacroBlockId &macro_id = object_handle.get_macro_id();
      ASSERT_TRUE(macro_id.is_valid());
      macro_ids.push_back(macro_id);
      OK(lru.put(object_handle, DATA_SIZE));
    }
    object_handle.reset();

    ASSERT_EQ(OB_STORAGE_OBJECT_MGR.get_macro_block_size() * MACRO_NUM, lru.disk_size_allocated());
    ASSERT_EQ(DATA_SIZE * MACRO_NUM, lru.disk_size_in_use());
    ASSERT_EQ(MACRO_NUM, lru.get_lru_entry_alloc_cnt_());
    for (int64_t i = 0; i < MACRO_NUM; i++) {
      ObMacroBlockInfo macro_block_info;
      ObMacroBlockHandle macro_block_handle;
      OK(OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_ids[i], macro_block_info, macro_block_handle));
      ASSERT_EQ(1, macro_block_info.ref_cnt_);
    }

    // check postion
    bool is_exist = false;
    ObPCachedExtLRUListEntry *cur = nullptr;
    for (int64_t i = 0; i < MACRO_NUM; i++) {
      for (int64_t access_cnt = ObPCachedExtLRUListEntry::UPDATE_LRU_LIST_THRESHOLD; access_cnt > 0; access_cnt--) {
        is_exist = false;
        uint32_t data_size = 0;
        OK(lru.get(macro_ids[i], is_exist, object_handle, data_size));
        ASSERT_EQ(DATA_SIZE, data_size);
        ASSERT_TRUE(is_exist);
        object_handle.reset_macro_id();

        // before access UPDATE_LRU_LIST_THRESHOLD, lru entry will not be moved to head
        if (access_cnt > 1) {
          ASSERT_NE(macro_ids[i], lru.lru_list_.get_first()->get_macro_id());
        } else {
          ASSERT_EQ(macro_ids[i], lru.lru_list_.get_first()->get_macro_id());
        }
      }
    }

    // test evict
    int64_t actual_evict_num = 0;
    // normal evict
    OK(lru.evict(MACRO_NUM, actual_evict_num));
    ASSERT_EQ(0, actual_evict_num);
    // force evict
    OK(lru.evict(MACRO_NUM, actual_evict_num, true/*force_evict*/));
    ASSERT_EQ(MACRO_NUM, actual_evict_num);
    ASSERT_EQ(0, lru.disk_size_allocated());
    ASSERT_EQ(0, lru.disk_size_in_use());
    ASSERT_EQ(0, lru.get_lru_entry_alloc_cnt_());
    for (int64_t i = 0; i < MACRO_NUM; i++) {
      ObMacroBlockInfo macro_block_info;
      ObMacroBlockHandle macro_block_handle;
      OK(OB_SERVER_BLOCK_MGR.get_macro_block_info(macro_ids[i], macro_block_info, macro_block_handle));
      ASSERT_EQ(0, macro_block_info.ref_cnt_);
    }
  }
}

TEST_F(TestSNPCachedExternalFileService, basic_test)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    // read external table file, no buffered read
    const int64_t read_buf_size = 100;
    char read_buf[read_buf_size] = { 0 };
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, 0/*offset*/,
        external_file_info, external_read_info));
    external_read_info.io_desc_.set_buffered_read(false);
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    // buffered read
    external_read_info.io_desc_.set_buffered_read(true);
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSNPCachedExternalFileService, test_prefetch_task_basic)
{
  if (enable_test_) {
    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    // init
    const int64_t read_buf_size = 100;
    char read_buf[read_buf_size] = { 0 };
    const int64_t offset = 2 * MB + 7;
    ASSERT_TRUE(offset + read_buf_size < content_size);
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, offset,
        external_file_info, external_read_info));

    // check macro block not cached
    MacroBlockId macro_id;
    const int64_t modify_time = external_file_info.modify_time_;
    bool is_cached = false;
    ObStorageObjectHandle macro_handle;
    ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, offset);
    ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_FALSE(is_cached);
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_FALSE(is_cached);

    // read external table file
    ObStorageObjectHandle io_handle;
    OK(accesser->async_read(external_file_info, external_read_info, io_handle));
    ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_FALSE(is_cached);
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    LOG_INFO("read external table file", K(accesser->timer_task_scheduler_));

    // wait for read completion
    OK(io_handle.wait());
    // check read result
    ASSERT_EQ(read_buf_size, io_handle.get_data_size());
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + offset, read_buf_size));
    LOG_INFO("wait for read completion", K(accesser->timer_task_scheduler_));

    // check read task
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(1, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    const int64_t start_time_us = ObTimeUtility::current_time();
    const int64_t MAX_WAIT_TIME_US = 10 * S_US;  // 10s
    const int64_t READ_TIME_LIMIT_US = ObTimeUtility::current_time() + MAX_WAIT_TIME_US;
    while (0 < accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total()
        && ObTimeUtility::current_time() < READ_TIME_LIMIT_US) {
      ob_usleep(10 * MS_US); // 10ms
      accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    }
    LOG_INFO("check read task", K(accesser->timer_task_scheduler_));

    // check write task
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(0, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    ASSERT_EQ(1, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_write_list_.get_curr_total());
    const int64_t WRITE_TIME_LIMIT_US = ObTimeUtility::current_time() + MAX_WAIT_TIME_US;
    while (0 < accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_write_list_.get_curr_total()
        && ObTimeUtility::current_time() < WRITE_TIME_LIMIT_US) {
      ob_usleep(10 * MS_US); // 10ms
      accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    }
    LOG_INFO("check write task", K(accesser->timer_task_scheduler_));
    ASSERT_EQ(0, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_write_list_.get_curr_total());

    // check finish task
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    // check macro block has cached
    OK(accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_TRUE(is_cached);
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_TRUE(is_cached);
    ASSERT_EQ(macro_id, macro_handle.get_macro_id());
    uint32_t data_size = 0;
    OK(accesser->lru_.get(macro_id, is_cached, macro_handle, data_size));
    ASSERT_EQ(2 * MB, data_size);
    LOG_INFO("check macro block has cached", K(macro_id), K(external_file_info), K(external_read_info));

    // read again
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));

    // read task should not be generated
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(0, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_));

    // test invalid read range
    // 1. read size > macro block size
    external_read_info.offset_ = 0;
    external_read_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size() + 1;
    ASSERT_TRUE(external_read_info.is_valid());
    ASSERT_EQ(OB_INVALID_ARGUMENT, accesser->async_read(external_file_info, external_read_info, io_handle));
    // 2. read range across macro block boundary
    external_read_info.offset_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size() - 1;
    external_read_info.size_ = 2;
    ASSERT_TRUE(external_read_info.is_valid());
    ASSERT_EQ(OB_INVALID_ARGUMENT, accesser->async_read(external_file_info, external_read_info, io_handle));
    ASSERT_TRUE(ObPCachedExternalFileService::is_read_range_valid(OB_STORAGE_OBJECT_MGR.get_macro_block_size() - 1, 1));
    // 3. test object manager read range
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size() - 1;
    read_info.size_ = 2;
    read_info.buf_ = read_buf;
    read_info.io_desc_ = external_read_info.io_desc_;
    ASSERT_TRUE(read_info.is_valid());
    ASSERT_EQ(OB_INVALID_ARGUMENT, OB_STORAGE_OBJECT_MGR.async_read_object(read_info, io_handle));

    read_info.offset_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size() + 1;
    ASSERT_TRUE(read_info.is_valid());
    ASSERT_EQ(OB_INVALID_ARGUMENT, OB_STORAGE_OBJECT_MGR.async_read_object(read_info, io_handle));

    {
      // read tail block, cached size should be content_size % 2MB = 7;
      const int64_t tail_block_size = content_size % (2 * MB);
      ASSERT_TRUE(tail_block_size > 0);
      ASSERT_TRUE(tail_block_size < 2 * MB);
      const int64_t tail_block_offset = content_size - tail_block_size;
      const int64_t tail_block_read_buf_size = tail_block_size + 10;
      char tail_block_read_buf[tail_block_read_buf_size] = { 0 };

      external_read_info.offset_ = tail_block_offset;
      external_read_info.size_ = tail_block_read_buf_size;
      external_read_info.buffer_ = tail_block_read_buf;
      ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, tail_block_offset);
      LOG_INFO("read tail block", K(macro_key), K(tail_block_offset), K(tail_block_read_buf_size), K(macro_id));
      ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
      print_map(accesser->path_map_.path_id_map_);
      print_map(accesser->path_map_.macro_id_map_);

      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
      OK(read_and_check(external_file_info, external_read_info, write_buf, tail_block_size));
      ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
      // prefetch任务成功后才会插入path map，所以这里应该是1
      ASSERT_EQ(1, accesser->path_map_.macro_id_map_.size());

      // finish prefetch task
      ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
      OK(wait_prefetch_task_finish());
      LOG_INFO("finish read task", K(accesser->timer_task_scheduler_),
          K(external_read_info), K(external_file_info));
      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

      // check macro block has cached
      macro_id.reset();
      OK(accesser->path_map_.get(macro_key, macro_id));
      OK(accesser->check_macro_cached(macro_key, is_cached));
      ASSERT_TRUE(is_cached);
      OK(accesser->get_cached_macro(macro_key, tail_block_offset, 1/*read_size*/, is_cached, macro_handle));
      ASSERT_TRUE(is_cached);
      ASSERT_EQ(macro_id, macro_handle.get_macro_id());
      data_size = 0;
      OK(accesser->lru_.get(macro_id, is_cached, macro_handle, data_size));
      ASSERT_EQ(tail_block_size, data_size);
      ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
      ASSERT_EQ(2, accesser->path_map_.macro_id_map_.size());

      // read again
      ASSERT_EQ(OB_INVALID_ARGUMENT, read_and_check(external_file_info, external_read_info, write_buf, tail_block_size));
      // read must use read size
      external_read_info.size_ = tail_block_size;
      OK(read_and_check(external_file_info, external_read_info, write_buf, tail_block_size));

      // read task should not be generated
      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
      ASSERT_EQ(0, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
      LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_),
          K(external_read_info), K(external_file_info));
    }

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSNPCachedExternalFileService, test_read_complete_macro)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    const int64_t read_buf_size = 2 * MB;
    char read_buf[read_buf_size] = { 0 };
    const int64_t offset = 0;
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, offset,
        external_file_info, external_read_info));

    // check macro block not cached
    MacroBlockId macro_id;
    const int64_t modify_time = external_file_info.modify_time_;
    bool is_cached = false;
    ObStorageObjectHandle macro_handle;
    ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, offset);
    ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_FALSE(is_cached);
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_FALSE(is_cached);

    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(0, accesser->path_map_.macro_id_map_.size());

    // check read task
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(1, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    const int64_t start_time_us = ObTimeUtility::current_time();
    const int64_t MAX_WAIT_TIME_US = 10 * S_US;  // 10s
    ObStorageIOPipelineTask<ObSNExternalDataPrefetchTaskInfo> *task = nullptr;
    OK(accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.head_unsafe(task));
    ASSERT_NE(nullptr, task);
    ASSERT_EQ(ObExternalDataPrefetchTaskInfo::DIRECT_WRITE, task->info_.operation_type_);

    ASSERT_EQ(TaskState::TASK_READ_IN_PROGRESS, task->info_.get_state());
    accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    ASSERT_EQ(TaskState::TASK_WRITE_IN_PROGRESS, task->info_.get_state());
    OK(wait_prefetch_task_finish());

    // check macro block has cached
    OK(accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_TRUE(is_cached);
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_TRUE(is_cached);
    ASSERT_EQ(macro_id, macro_handle.get_macro_id());
    LOG_INFO("check macro block has cached", K(macro_id), K(external_file_info), K(external_read_info), K(macro_id));

    // read again
    MEMSET(read_buf, 0, read_buf_size);
    external_read_info.offset_ = 10;
    external_read_info.size_ = 100;
    external_read_info.buffer_ = read_buf;
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));

    // read task should not be generated
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(0, accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_),
        K(external_read_info), K(external_file_info));

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSNPCachedExternalFileService, test_clean_path_map)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    {
      ObExternalFilePathMap path_map;
      OK(path_map.init(MTL_ID()));
      const ObString url_a("s3://bucket/path/a");
      const ObString url_b("s3://bucket/path/b");
      const ObString url_c("s3://bucket/path/c");
      const ObPCachedExtMacroKey macro_key_1(url_a, "", 100, 0);
      const ObPCachedExtMacroKey macro_key_2(url_a, "", 100, 2 * MB);
      const ObPCachedExtMacroKey macro_key_3(url_b, "", 100, 4 * MB);
      const ObPCachedExtMacroKey macro_key_4(url_c, "", 100, 0);

      MacroBlockId macro_id_1;
      ObStorageObjectHandle macro_handle_1;
      OK(path_map.alloc_macro(macro_key_1, macro_handle_1));
      OK(path_map.overwrite(macro_key_1, macro_handle_1));
      macro_id_1 = macro_handle_1.get_macro_id();
      MacroBlockId macro_id_2;
      ObStorageObjectHandle macro_handle_2;
      OK(path_map.alloc_macro(macro_key_2, macro_handle_2));
      OK(path_map.overwrite(macro_key_2, macro_handle_2));
      macro_id_2 = macro_handle_2.get_macro_id();
      MacroBlockId macro_id_3;
      ObStorageObjectHandle macro_handle_3;
      OK(path_map.alloc_macro(macro_key_3, macro_handle_3));
      OK(path_map.overwrite(macro_key_3, macro_handle_3));
      macro_id_3 = macro_handle_3.get_macro_id();
      MacroBlockId macro_id_4;
      ObStorageObjectHandle macro_handle_4;
      OK(path_map.alloc_macro(macro_key_4, macro_handle_4));
      macro_id_4 = macro_handle_4.get_macro_id();
      MacroBlockId macro_id_5;
      const ObExternalFilePathMap::MacroMapKey macro_map_key_5(UINT64_MAX - 1, 1);
      OK(path_map.get_or_generate_macro_id_(macro_map_key_5, macro_id_5));

      // check map size
      ASSERT_EQ(3, path_map.path_id_map_.size());
      ASSERT_EQ(4, path_map.macro_id_map_.size());

      // clean orphaned path
      ObExternalFileReversedPathMap reversed_path_map;
      OK(reversed_path_map.create(path_map.bucket_count(), ObMemAttr(MTL_ID(), "ExtRvsPathMap")));
      OK(path_map.build_reversed_path_mapping(reversed_path_map));

      MacroBlockId tmp_macro_id;
      const ObPCachedExtMacroKey *tmp_macro_key = nullptr;
      OK(path_map.get(macro_key_1, tmp_macro_id));
      ASSERT_EQ(macro_id_1, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_1);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_a, tmp_macro_key->url());

      OK(path_map.get(macro_key_2, tmp_macro_id));
      ASSERT_EQ(macro_id_2, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_2);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_a, tmp_macro_key->url());

      OK(path_map.get(macro_key_3, tmp_macro_id));
      ASSERT_EQ(macro_id_3, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_3);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_b, tmp_macro_key->url());

      ASSERT_EQ(3, reversed_path_map.size());
      ASSERT_EQ(2, path_map.path_id_map_.size());
      ASSERT_EQ(3, path_map.macro_id_map_.size());

      OK(path_map.erase_orphaned_macro_id(macro_key_1));
      OK(path_map.erase_orphaned_macro_id(macro_key_2));
      OK(path_map.erase_orphaned_macro_id(macro_key_3));
      OK(path_map.build_reversed_path_mapping(reversed_path_map));
      ASSERT_TRUE(reversed_path_map.empty());
    }

    // generate external table file
    ObArenaAllocator allocator;
    const int64_t content_size = 4 * MB + 7;
    char ext_file_a[OB_MAX_URI_LENGTH] = { 0 };
    char ext_file_b[OB_MAX_URI_LENGTH] = { 0 };
    char ext_file_c[OB_MAX_URI_LENGTH] = { 0 };
    char *write_buf_a = nullptr;
    char *write_buf_b = nullptr;
    char *write_buf_c = nullptr;
    OK(generate_external_file(allocator, content_size,
        ext_file_a, sizeof(ext_file_a), write_buf_a));
    OK(generate_external_file(allocator, content_size,
        ext_file_b, sizeof(ext_file_b), write_buf_b));
    OK(generate_external_file(allocator, content_size,
        ext_file_c, sizeof(ext_file_c), write_buf_c));

    // cache data
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    // b: [4MB, 7) only in macro id map
    // c: [2MB, 4MB), [4MB, 7); only in path map (means not cached)
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    const int64_t read_buf_size = 2 * MB;
    char read_buf[read_buf_size] = { 0 };
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, ext_file_a, read_buf, read_buf_size, 0/*offset*/,
        external_file_info, external_read_info));
    // cache a
    external_read_info.size_ = 100;
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    external_read_info.offset_ = 2 * MB;
    external_read_info.size_ = 2 * MB;
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    // check prefetch task
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    // cache b
    external_file_info.url_ = ext_file_b;
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_b, external_read_info.size_));
    // cache c
    external_file_info.url_ = ext_file_c;
    external_read_info.offset_ = 2 * MB;
    external_read_info.size_ = 2 * MB;
    OK(read_and_check(external_file_info, external_read_info, write_buf_c, external_read_info.size_));
    external_file_info.url_ = ext_file_c;
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_c, external_read_info.size_));
    // check prefetch task
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    // check blocks in path map
    ObPCachedExtMacroKey tmp_macro_key(ext_file_a, "", external_file_info.modify_time_, 0/*offset*/);
    MacroBlockId tmp_macro_id;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_b;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));

    // generate other blocks
    // b: [4MB, 7) only in macro id map
    tmp_macro_key.url_ = ext_file_b;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.path_id_map_.erase_refactored(tmp_macro_key));
    LOG_INFO("print_map =============================== begin");
    print_map(accesser->path_map_.path_id_map_);
    print_map(accesser->path_map_.macro_id_map_);
    // c: [2MB, 4MB), [4MB, 7); only in path map (means not cached)
    // evict c [2MB, 4MB)
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    bool is_cached = false;
    OK(accesser->check_macro_cached(tmp_macro_key, is_cached));
    ASSERT_TRUE(is_cached);
    bool is_erased = false;
    OK(accesser->lru_.erase(tmp_macro_id, is_erased));
    ASSERT_TRUE(is_erased);
    OK(accesser->check_macro_cached(tmp_macro_key, is_cached));
    ASSERT_FALSE(is_cached);
    // evict c [4MB, 7)
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    OK(accesser->check_macro_cached(tmp_macro_key, is_cached));
    ASSERT_TRUE(is_cached);
    OK(accesser->lru_.erase(tmp_macro_id, is_erased));
    ASSERT_TRUE(is_erased);
    OK(accesser->check_macro_cached(tmp_macro_key, is_cached));
    ASSERT_FALSE(is_cached);

    // clean path map
    accesser->timer_task_scheduler_.cleanup_task_.runTimerTask();
    // remained blocks:
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    // c: only in path id map
    ASSERT_EQ(2, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(3, accesser->path_map_.macro_id_map_.size());
    // clean again
    accesser->timer_task_scheduler_.cleanup_task_.runTimerTask();
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(3, accesser->path_map_.macro_id_map_.size());

    OK(ObBackupIoAdapter::del_file(ext_file_a, &info_base_));
    OK(ObBackupIoAdapter::del_file(ext_file_b, &info_base_));
    OK(ObBackupIoAdapter::del_file(ext_file_c, &info_base_));
  }
}

TEST_F(TestSNPCachedExternalFileService, test_disk_space_manager)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    const int64_t init_cached_macro_count = OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count();
    const int64_t init_disk_size_allocated = OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated();

    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    const int64_t read_buf_size = 2 * MB;
    char read_buf[read_buf_size] = { 0 };
    const int64_t offset = 0;
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, offset,
        external_file_info, external_read_info));

    // check macro block not cached
    MacroBlockId macro_id;
    const int64_t modify_time = external_file_info.modify_time_;
    bool is_cached = false;
    ObStorageObjectHandle macro_handle;
    ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, offset);
    ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
    OK(accesser->check_macro_cached(macro_key, is_cached));
    ASSERT_FALSE(is_cached);
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_FALSE(is_cached);

    // read external table file
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    OK(accesser->get_cached_macro(macro_key, offset, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_TRUE(is_cached);

    // test space manager clean
    ASSERT_EQ(OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count(), init_cached_macro_count + 1);
    ASSERT_EQ(OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated(), init_disk_size_allocated + 2 * MB);
    // force evict == false, should not evict
    GCONF.external_table_disk_cache_max_percentage = 0;
    ASSERT_FALSE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.is_total_disk_usage_exceed_limit_());
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.is_external_file_disk_space_full_());
    OK(OB_EXTERNAL_FILE_DISK_SPACE_MGR.start());
    ob_usleep(2 * OB_EXTERNAL_FILE_DISK_SPACE_MGR.CHECK_DISK_USAGE_INTERVAL_US);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count() == init_cached_macro_count + 1);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated() == init_disk_size_allocated + 2 * MB);

    // async read another block, should not generate prefetch task
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    // wait for LRU entry to exceed minimum retention duration (30s) to allow space manager cleanup
    ob_usleep(ObPCachedExtLRU::MIN_RETENTION_DURATION_US);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count() == 0);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated() == 0);

    // test force evict
    GCONF.external_table_disk_cache_max_percentage = 95;
    // read external table file
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    macro_key.offset_ = external_read_info.offset_;
    OK(accesser->get_cached_macro(macro_key, external_read_info.offset_, 1/*read_size*/, is_cached, macro_handle));
    ASSERT_TRUE(is_cached);

    // should not evict
    ob_usleep(2 * OB_EXTERNAL_FILE_DISK_SPACE_MGR.CHECK_DISK_USAGE_INTERVAL_US);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count() == 1);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated() == 2 * MB);

    ObArray<ObStorageObjectHandle> macro_handles;
    while (!OB_EXTERNAL_FILE_DISK_SPACE_MGR.is_total_disk_usage_exceed_limit_()) {
      ObStorageObjectHandle tmp_handle;
      ObStorageObjectOpt opt;
      opt.set_ss_external_table_file_opt(1, 1); // args is not used in SN mode
      OK(OB_STORAGE_OBJECT_MGR.alloc_object(opt, tmp_handle));
      OK(macro_handles.push_back(tmp_handle));
    }
    ASSERT_FALSE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.is_external_file_disk_space_full_());
    ob_usleep(2 * OB_EXTERNAL_FILE_DISK_SPACE_MGR.CHECK_DISK_USAGE_INTERVAL_US);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.cached_macro_count() == 0);
    ASSERT_TRUE(OB_EXTERNAL_FILE_DISK_SPACE_MGR.disk_size_allocated() == 0);

    for (int64_t i = 0; i < macro_handles.count(); ++i) {
      ObStorageObjectHandle &tmp_handle = macro_handles.at(i);
      MacroBlockId macro_id = tmp_handle.get_macro_id();
      tmp_handle.reset_macro_id();
      OK(OB_SERVER_BLOCK_MGR.sweep_one_block(macro_id));
    }
    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSNPCachedExternalFileService, test_external_file_access)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    // read without disk cache
    ObExternalFileAccess file_access;
    char access_info[OB_MAX_URI_LENGTH] = { 0 };
    ObObjectStorageInfo storage_info;
    OK(storage_info.assign(info_base_));
    OK(storage_info.get_storage_info_str(access_info, sizeof(access_info)));
    const ObExternalFileUrlInfo file_url_info(external_file_name, access_info, external_file_name,
        ""/*content_digest*/, content_size, 1/*modify_time*/);
    ObExternalFileCacheOptions cache_options(false/*enable_page_cache*/, false/*enable_disk_cache*/);
    OK(file_access.open(file_url_info, cache_options));

    char read_buf[content_size] = { 0 };
    // 跨macro读
    int64_t offset = 3 * MB - 1;
    ObExternalReadInfo read_info(offset, read_buf, content_size - offset, 10000/*io_timeout_ms*/);
    int64_t read_size = -1;
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));

    // read with disk cache
    file_access.reset();
    cache_options.set_enable_disk_cache();
    read_info.offset_ = 0;
    read_info.size_ = content_size;
    OK(file_access.open(file_url_info, cache_options));
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));

    // check prefetch task
    // [0, 2MB), [2MB, 4MB), [4MB, 7)
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());

    // read with disk cache again, disable page cache
    file_access.reset();
    cache_options.enable_page_cache_ = false;
    OK(file_access.open(file_url_info, cache_options));
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

// TODO @fangdan: 增加errsim测试

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_sn_pcached_external_file_service.log*");
  oceanbase::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  OB_LOGGER.set_file_name("test_sn_pcached_external_file_service.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}