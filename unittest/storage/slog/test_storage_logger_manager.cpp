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

#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/slog/simple_ob_storage_log.h"
#include "storage/slog/ob_storage_log_batch_header.h"

#define private public
#undef private

namespace oceanbase
{
using namespace common;
static ObSimpleMemLimitGetter getter;

namespace storage
{

class TestStorageLoggerManager : public TestDataFilePrepare
{
public:
  TestStorageLoggerManager()
    : TestDataFilePrepare(&getter, "TestStorageLoggerManager")
  {
  }
  virtual ~TestStorageLoggerManager() {}
  TestStorageLoggerManager(const TestStorageLoggerManager &) = delete;
  TestStorageLoggerManager &operator = (const TestStorageLoggerManager &) = delete;

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  const int64_t MAX_CONCURRENT_ITEM_CNT = 1024;

public:
  ObLogCursor start_cursor_;
  blocksstable::ObLogFileSpec log_file_spec_;
};

void TestStorageLoggerManager::SetUp()
{
  start_cursor_.file_id_ = 1;
  start_cursor_.log_id_ = 1;
  start_cursor_.offset_ = 0;
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
  TestDataFilePrepare::TearDown();
  TestDataFilePrepare::SetUp();
}

void TestStorageLoggerManager::TearDown()
{
  TestDataFilePrepare::TearDown();
}

TEST_F(TestStorageLoggerManager, test_manager_basic)
{
  int ret = OB_SUCCESS;

  // test invalid init
  ObStorageLoggerManager &slogger_mgr = SERVER_STORAGE_META_SERVICE.get_slogger_manager();
  slogger_mgr.destroy();
  ret = slogger_mgr.init(nullptr, nullptr, MAX_FILE_SIZE, log_file_spec_);
  ASSERT_NE(OB_SUCCESS, ret);
  // test normal init
  ret = slogger_mgr.init(OB_FILE_SYSTEM_ROUTER.get_slog_dir(), OB_FILE_SYSTEM_ROUTER.get_slog_dir(),  MAX_FILE_SIZE, log_file_spec_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(slogger_mgr.need_reserved_);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.log_buffers_.capacity());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.slog_items_.capacity());

  ObStorageLogItem *log_item = nullptr;
  ObStorageLogItem *log_item_local = nullptr;
  // test invalid item allocation
  ret = slogger_mgr.alloc_item(ObLogConstants::LOG_ITEM_MAX_LENGTH+100,
      log_item, 1);
  ASSERT_NE(OB_SUCCESS, ret);
  // test normal item allocation (not local)
  ret = slogger_mgr.alloc_item(3 * 1024, log_item, 15);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(log_item->is_inited_);
  ASSERT_FALSE(log_item->is_local_);
  // test normal item allocation (local)
  ret = slogger_mgr.alloc_item(513 * 1024, log_item_local, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(log_item_local->is_inited_);
  ASSERT_TRUE(log_item_local->is_local_);

  // test invalid item free
  ret = slogger_mgr.free_item(nullptr);
  ASSERT_NE(OB_SUCCESS, ret);
  // test normal item free
  ret = slogger_mgr.free_item(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = slogger_mgr.free_item(log_item_local);
  ASSERT_EQ(OB_SUCCESS, ret);

  slogger_mgr.destroy();
}

TEST_F(TestStorageLoggerManager, test_slogger_basic)
{
  int ret = OB_SUCCESS;
  ObLogCursor cursor;
  cursor.file_id_ = 3;
  cursor.log_id_ = 5;
  cursor.offset_ = 500;
  ObStorageLoggerManager &slogger_mgr = SERVER_STORAGE_META_SERVICE.get_slogger_manager();

  ObStorageLogger *slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, slogger->init(slogger_mgr, 1));
  ASSERT_EQ(OB_SUCCESS, slogger->start());

  slogger->start_log(cursor);

  // test get_active_cursor
  ObLogCursor tmp_cursor;
  ret = slogger->get_active_cursor(tmp_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_cursor.file_id_, cursor.file_id_);
  ASSERT_EQ(tmp_cursor.log_id_, cursor.log_id_);
  ASSERT_EQ(tmp_cursor.offset_, cursor.offset_);

  // test get_using_disk_space
  int64_t space;
  ret = slogger->get_using_disk_space(space);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, space);

  // test get_start_file_id
  int64_t start_id = 0;
  ret = slogger->get_start_file_id(start_id, 5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(cursor.file_id_, start_id);

  // test normal file remove
  ret = slogger->remove_useless_log_file(cursor.file_id_+1, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  slogger->get_start_file_id(start_id, 5);
  ASSERT_EQ(1, start_id);

  slogger->destroy();
  slogger_mgr.destroy();
}

TEST_F (TestStorageLoggerManager, test_build_item)
{
  int ret = OB_SUCCESS;

  ObStorageLoggerManager &slogger_mgr = SERVER_STORAGE_META_SERVICE.get_slogger_manager();
  ObStorageLogger *slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, slogger->init(slogger_mgr, 1));
  ASSERT_EQ(OB_SUCCESS, slogger->start());

  slogger->start_log(start_cursor_);

  ObStorageLogParam slog_param;
  ObStorageLogItem *log_item;
  slog_param.cmd_ = 33;

  ObStorageLogBatchHeader dummy_header;
  ObStorageLogEntry dummy_entry;
  int64_t data_len;
  int64_t buf_size;


  // test build single-param normal-size item
  SimpleObSlog simple_slog1(12, 'A');
  slog_param.data_ = &simple_slog1;
  ret = slogger->build_log_item(slog_param, log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT - 1, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT - 1, slogger_mgr.log_buffers_.get_curr_total());
  data_len = dummy_header.get_serialize_size() +
            dummy_entry.get_serialize_size() +
            12;
  buf_size = 8<<10;
  ASSERT_EQ(data_len, log_item->get_data_len());
  ASSERT_EQ(data_len, log_item->get_log_data_len());
  ASSERT_EQ(buf_size, log_item->get_buf_size());

  // free item
  ret = slogger_mgr.free_item(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.log_buffers_.get_curr_total());

  // test build single-param large-size item
  SimpleObSlog simple_slog2(512<<10, 't');
  slog_param.data_ = &simple_slog2;
  ret = slogger->build_log_item(slog_param, log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT - 1, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.log_buffers_.get_curr_total());
  data_len = dummy_header.get_serialize_size() +
             dummy_entry.get_serialize_size() +
             (512<<10);
  int64_t unaligned_size = data_len +
                         dummy_header.get_serialize_size() +
                         dummy_entry.get_serialize_size() +
                         ObLogConstants::LOG_FILE_ALIGN_SIZE;
  buf_size = upper_align(unaligned_size, ObLogConstants::LOG_FILE_ALIGN_SIZE);
  ASSERT_EQ(data_len, log_item->get_data_len());
  ASSERT_EQ(data_len, log_item->get_log_data_len());
  ASSERT_EQ(buf_size, log_item->get_buf_size());

  // free item
  ret = slogger_mgr.free_item(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.log_buffers_.get_curr_total());

  ObStorageLogParam slog_param_batch1;
  slog_param_batch1.cmd_ = 39;
  ObStorageLogParam slog_param_batch2;
  slog_param_batch2.cmd_ = 39;
  ObStorageLogParam slog_param_batch3;
  slog_param_batch3.cmd_ = 39;
  ObSEArray<ObStorageLogParam, 3> param_arr;
  // test build batch-param item
  SimpleObSlog simple_slog_batch1(32, 'a');
  slog_param_batch1.data_ = &simple_slog_batch1;
  SimpleObSlog simple_slog_batch2(78, 'z');
  slog_param_batch2.data_ = &simple_slog_batch2;
  SimpleObSlog simple_slog_batch3(1, 'd');
  slog_param_batch3.data_ = &simple_slog_batch3;
  param_arr.push_back(slog_param_batch1);
  param_arr.push_back(slog_param_batch2);
  param_arr.push_back(slog_param_batch3);
  ret = slogger->build_log_item(param_arr, log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT - 1, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT - 1, slogger_mgr.log_buffers_.get_curr_total());
  data_len = dummy_header.get_serialize_size() +
             3 * dummy_entry.get_serialize_size() +
             111;
  buf_size = 8<<10;
  ASSERT_EQ(data_len, log_item->get_data_len());
  ASSERT_EQ(data_len, log_item->get_log_data_len());
  ASSERT_EQ(buf_size, log_item->get_buf_size());

  // free item
  ret = slogger_mgr.free_item(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.slog_items_.get_curr_total());
  ASSERT_EQ(MAX_CONCURRENT_ITEM_CNT, slogger_mgr.log_buffers_.get_curr_total());

  slogger->destroy();
  slogger_mgr.destroy();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_logger_manager.log*");
  OB_LOGGER.set_file_name("test_storage_logger_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
