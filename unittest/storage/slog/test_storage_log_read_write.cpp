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

#define USING_LOG_PREFIX STORAGE_REDO

#include <gtest/gtest.h>
#include "lib/list/ob_list.h"
#include "lib/file/file_directory_utils.h"
#include "lib/file/ob_file.h"
#include "lib/ob_define.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/slog/simple_ob_storage_log.h"
#include "storage/slog/ob_storage_log_batch_header.h"
#include "lib/container/ob_se_array.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include <thread>
#include "lib/oblog/ob_log.h"
#include "share/ob_force_print_log.h"

#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/palf_options.h"
#include "share/ob_alive_server_tracer.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "lib/file/file_directory_utils.h"
#include "storage/mock_ob_meta_report.h"
#include "share/ob_simple_mem_limit_getter.h"

#define private public
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log_writer.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_log_replayer.h"
#undef private

namespace oceanbase
{
using namespace common;
static ObSimpleMemLimitGetter getter;

namespace storage
{

class TestStorageLogRW : public TestDataFilePrepare
{
public:
  TestStorageLogRW()
    : TestDataFilePrepare(&getter, "TestStorageLogRW"),
      tenant_base1_(OB_SERVER_TENANT_ID),
      slogger_(nullptr)
  {
  }
  virtual ~TestStorageLogRW() {}
  TestStorageLogRW(const TestStorageLogRW &) = delete;
  TestStorageLogRW &operator = (const TestStorageLogRW &) = delete;

  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  const int64_t MAX_CONCURRENT_ITEM_CNT = !lib::is_mini_mode() ?
      MAX(128, sysconf(_SC_NPROCESSORS_ONLN) * 2) : 64;

public:
  char dir_[128];
  ObTenantBase tenant_base1_;
  ObLogCursor start_cursor_;
  blocksstable::ObLogFileSpec log_file_spec_;
  ObStorageLogBatchHeader dummy_header_;
  ObStorageLogEntry dummy_entry_;
  ObStorageLogger *slogger_;
};

void TestStorageLogRW::SetUp()
{
  system("rm -rf ./test_storage_log_read_write");
  MEMCPY(dir_, "./test_storage_log_read_write", sizeof("./test_storage_log_read_write"));
  start_cursor_.file_id_ = 1;
  start_cursor_.log_id_ = 1;
  start_cursor_.offset_ = 0;
  log_file_spec_.retry_write_policy_ = "normal";
  log_file_spec_.log_create_policy_ = "normal";
  log_file_spec_.log_write_policy_ = "truncate";
  TestDataFilePrepare::TearDown();
  TestDataFilePrepare::SetUp();
  FileDirectoryUtils::create_full_path("./test_storage_log_read_write");
  SLOGGERMGR.destroy();
  SLOGGERMGR.init(dir_, dir_, MAX_FILE_SIZE, log_file_spec_);

  ObStorageLogger *tmp_slogger = OB_NEW(ObStorageLogger, ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->init(SLOGGERMGR, OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, tmp_slogger->start());

  tenant_base1_.set(tmp_slogger);
  ObTenantEnv::set_tenant(&tenant_base1_);
  ASSERT_EQ(OB_SUCCESS, tenant_base1_.init());

  slogger_ = MTL(ObStorageLogger*);
  slogger_->start_log(start_cursor_);
}

void TestStorageLogRW::TearDown()
{
  MTL(ObStorageLogger*)->destroy();
  SLOGGERMGR.destroy();
  system("rm -rf ./test_storage_log_read_write");
  TestDataFilePrepare::TearDown();
}

TEST_F(TestStorageLogRW, test_basic)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  ObMetaDiskAddr disk_addr;
  ObLogCursor output_cursor;
  ObStorageLogParam log_param;
  log_param.cmd_ = 29;

  int cmp = 0;
  int64_t pos = 0;


  // test single-param

  // test write signle-param normal-size log and read it

  // test write single-param normal-size log
  SimpleObSlog simple_slog1(62, 'T');
  log_param.data_ = &simple_slog1;
  ret = slogger_->write_log(log_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  disk_addr = log_param.disk_addr_;
  ASSERT_EQ(dummy_header_.get_serialize_size(), disk_addr.offset_);
  ASSERT_EQ(dummy_entry_.get_serialize_size() + 62, disk_addr.size_);
  slogger_->get_active_cursor(output_cursor);
  ASSERT_EQ(2, output_cursor.log_id_);
  ASSERT_EQ(62 + dummy_header_.get_serialize_size() + dummy_entry_.get_serialize_size(),
      output_cursor.offset_);

  // test read single-param normal-size log
  buf = ob_malloc(disk_addr.size_, "ReadBuf");
  ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, disk_addr.size_, buf, pos, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = MEMCMP(simple_slog1.buf_, (char *)buf + pos, 62);
  ASSERT_EQ(0, cmp);
  ob_free(buf);
  buf = nullptr;



  // test write single-param large-size log and read it

  static const int single_large_size = 40 << 20;
  // test write single-param large-size log
  SimpleObSlog simple_slog2(single_large_size, 'g');
  log_param.data_ = &simple_slog2;
  ret = slogger_->write_log(log_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  disk_addr = log_param.disk_addr_;
  ASSERT_EQ(4096 + dummy_header_.get_serialize_size(), disk_addr.offset_);
  ASSERT_EQ(dummy_entry_.get_serialize_size() + single_large_size, disk_addr.size_);
  slogger_->get_active_cursor(output_cursor);
  ASSERT_EQ(3, output_cursor.log_id_);
  ASSERT_EQ(4096 + single_large_size + dummy_header_.get_serialize_size() +
      dummy_entry_.get_serialize_size(), output_cursor.offset_);

  // test read single-param large-size log
  buf = ob_malloc(disk_addr.size_, "ReadBuf");
  ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, disk_addr.size_, buf, pos, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = MEMCMP(simple_slog2.buf_, (char *)buf + pos, single_large_size);
  ASSERT_EQ(0, cmp);
  ob_free(buf);
  buf = nullptr;



  // test multi-param
  int64_t data_len[10];
  char content[10];
  ObSEArray<ObStorageLogParam, 10> param_arr;
  int offset[10];

  // test write multi-param normal-size log and read it

  // test write multi-param normal-size log
  offset[0] = dummy_header_.get_serialize_size() + (single_large_size + (8<<10)); // header + previous log
  SimpleObSlog slog_arr1[10];
  for (int i = 0; i < 10; i++) {
    data_len[i] = ObRandom::rand(1, 10<<10);
    content[i] = (char) ObRandom::rand(10, 100);
    slog_arr1[i].set(data_len[i], content[i]);
    log_param.cmd_ = 35;
    log_param.data_ = &(slog_arr1[i]);
    param_arr.push_back(log_param);
    if (0 != i) {
      offset[i] = offset[i-1] + dummy_entry_.get_serialize_size() + data_len[i-1];
    }
  }
  ret = slogger_->write_log(param_arr);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < 10; i++) {
    disk_addr = param_arr.at(i).disk_addr_;
    ASSERT_EQ(offset[i], disk_addr.offset_);
    ASSERT_EQ(dummy_entry_.get_serialize_size() + data_len[i], disk_addr.size_);
  }
  slogger_->get_active_cursor(output_cursor);
  ASSERT_EQ(13, output_cursor.log_id_);
  ASSERT_EQ(offset[9] + dummy_entry_.get_serialize_size() + data_len[9], output_cursor.offset_);

  // test read multi-param normal-size log
  for (int i = 0; i < 10; i++) {
    disk_addr = param_arr.at(i).disk_addr_;
    buf = ob_malloc(disk_addr.size_, ObNewModIds::TEST);
    ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, disk_addr.size_, buf, pos, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    cmp = MEMCMP(slog_arr1[i].buf_, (char *)buf + pos, data_len[i]);
    ASSERT_EQ(0, cmp);
    ob_free(buf);
    buf = nullptr;
  }



  // test write multi-param large-size log and read it

  // test write multi-param large-size log
  SimpleObSlog slog_arr2[10];
  ObStorageLogger *slogger2 = nullptr;
  SLOGGERMGR.get_server_slogger(slogger2);

  slogger2->start_log(start_cursor_);
  offset[0] = dummy_header_.get_serialize_size();
  ObSEArray<ObStorageLogParam, 10> param_arr2;

  for (int i = 0; i < 10; i++) {
    data_len[i] = ObRandom::rand(60<<10, 100<<10);
    content[i] = (char) ObRandom::rand(10, 100);
    slog_arr2[i].set(data_len[i], content[i]);
    log_param.cmd_ = 33;
    log_param.data_ = &(slog_arr2[i]);
    param_arr2.push_back(log_param);
    if (0 != i) {
      offset[i] = offset[i-1] + dummy_entry_.get_serialize_size() + data_len[i-1];
    }
  }
  ret = slogger2->write_log(param_arr2);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < 10; i++) {
    disk_addr = param_arr2.at(i).disk_addr_;
    ASSERT_EQ(offset[i], disk_addr.offset_);
    ASSERT_EQ(dummy_entry_.get_serialize_size() + data_len[i], disk_addr.size_);
  }
  slogger2->get_active_cursor(output_cursor);
  ASSERT_EQ(11, output_cursor.log_id_);
  ASSERT_EQ(offset[9] + dummy_entry_.get_serialize_size() + data_len[9], output_cursor.offset_);

  // test read multi-param large-size log
  for (int i = 0; i < 10; i++) {
    disk_addr = param_arr2.at(i).disk_addr_;
    buf = ob_malloc(disk_addr.size_, ObNewModIds::TEST);
    ret = ObStorageLogReader::read_log(slogger2->get_dir(), disk_addr, disk_addr.size_, buf, pos, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    cmp = MEMCMP(slog_arr2[i].buf_, (char *)buf + pos, data_len[i]);
    ASSERT_EQ(0, cmp);
    ob_free(buf);
    buf = nullptr;
  }
}

TEST_F(TestStorageLogRW, test_iter_read)
{
  ObLogCursor read_cursor;
  read_cursor.file_id_ = 1;
  read_cursor.log_id_ = 1;
  read_cursor.offset_ = 0;
  char *read_buf = nullptr;
  ObStorageLogEntry entry;
  ObMetaDiskAddr disk_addr;

  // read empty file
  ObStorageLogReader empty_reader;
  ASSERT_EQ(OB_SUCCESS, empty_reader.init(dir_, read_cursor, log_file_spec_, OB_SERVER_TENANT_ID));
  ASSERT_EQ(OB_READ_NOTHING, empty_reader.read_log(entry, read_buf, disk_addr));


  // write multiple slogs and read them

  // write slogs
  SimpleObSlog slog_arr[20];
  int64_t data_len[20];
  char content[20];
  ObSEArray<ObStorageLogParam, 20> param_arr;

  ObStorageLogParam log_param;
  log_param.cmd_ = 29;
  for (int i = 0; i < 20; i++) {
    data_len[i] = ObRandom::rand(1<<10, 10<<20);
    content[i] = (char) ObRandom::rand(10, 100);
    slog_arr[i].set(data_len[i], content[i]);
    log_param.data_ = &(slog_arr[i]);
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(log_param));
    param_arr.push_back(log_param);
  }

  // read slogs one by one
  int ret = OB_SUCCESS;
  int64_t log_id = 1;
  ObStorageLogReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(slogger_->get_dir(), read_cursor, log_file_spec_, OB_SERVER_TENANT_ID));
  ObRedoLogMainType main_type;
  ObRedoLogSubType sub_type;
  while (OB_READ_NOTHING != ret)
  {
    ret = reader.read_log(entry, read_buf, disk_addr);
    ObIRedoModule::parse_cmd(entry.cmd_, main_type, sub_type);
    if (ObRedoLogMainType::OB_REDO_LOG_SYS != main_type && OB_READ_NOTHING != ret) {
      int64_t index = log_id - 1;
      ASSERT_EQ(log_param.cmd_, entry.cmd_);
      ASSERT_EQ(log_id, entry.seq_);
      ASSERT_EQ(param_arr[index].disk_addr_, disk_addr);
      ASSERT_EQ(0, MEMCMP(slog_arr[index].buf_, read_buf, data_len[index]));
      log_id++;
    }
  }
  ASSERT_EQ(21, log_id);
}

TEST_F(TestStorageLogRW, test_nop)
{
  ObMetaDiskAddr disk_addr;
  ObStorageLogParam log_param;
  log_param.cmd_ = 29;
  int64_t total_header_len;

  ObStorageLogReader reader;
  ObLogCursor read_cursor;
  read_cursor.file_id_ = 1;
  read_cursor.log_id_  = 1;
  read_cursor.offset_  = 0;
  ObStorageLogEntry entry;
  char *read_buf = nullptr;
  ObMetaDiskAddr read_disk_addr;

  // nop size is 4k
  total_header_len = 2 * dummy_header_.get_serialize_size() +
                     2 * dummy_entry_.get_serialize_size();
  SimpleObSlog simple_slog((512<<10) - total_header_len, 'k');
  log_param.data_ = &simple_slog;
  slogger_->write_log(log_param);
  slogger_->write_log(log_param);
  disk_addr = log_param.disk_addr_;
  ASSERT_EQ((516<<10) + dummy_header_.get_serialize_size(), disk_addr.offset_);
  ASSERT_EQ((512<<10) - total_header_len + dummy_entry_.get_serialize_size(), disk_addr.size_);

  ASSERT_EQ(OB_SUCCESS, reader.init(slogger_->get_dir(), read_cursor, log_file_spec_, OB_SERVER_TENANT_ID));
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, read_disk_addr));
  }
  ASSERT_EQ(0, entry.seq_);
  ASSERT_EQ((4<<10) + dummy_entry_.get_serialize_size(), read_disk_addr.size_);

  // no nop
  total_header_len = dummy_header_.get_serialize_size() +
                     dummy_entry_.get_serialize_size();
  SimpleObSlog simple_slog2((512<<10) - total_header_len, 'z');
  log_param.data_ = &simple_slog2;
  slogger_->write_log(log_param);
  disk_addr = log_param.disk_addr_;
  ASSERT_EQ(2 * (516<<10) + dummy_header_.get_serialize_size(), disk_addr.offset_);
  slogger_->write_log(log_param);
  disk_addr = log_param.disk_addr_;
  ASSERT_EQ(2 * (516<<10) + (512<<10) + dummy_header_.get_serialize_size(), disk_addr.offset_);
  ASSERT_EQ((512<<10) - total_header_len + dummy_entry_.get_serialize_size(), disk_addr.size_);

  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, read_disk_addr));
  }
  ASSERT_EQ(OB_READ_NOTHING, reader.read_log(entry, read_buf, read_disk_addr));
}

TEST_F(TestStorageLogRW, test_switch_file)
{
  int ret = OB_SUCCESS;

  ObLogCursor output_cursor;
  ObStorageLogParam log_param;
  log_param.cmd_ = 29;

  ObMetaDiskAddr disk_addr;
  int cmp = 0;
  void *buf = nullptr;

  SimpleObSlog simple_slog((32<<20) - (10<<10), 'p');
  log_param.data_ = &simple_slog;

  for (int i = 0; i < 8; i++) { // to reach the max size of file
    slogger_->write_log(log_param);
  }

  int64_t data_len[100];
  char content[100];
  ObSEArray<ObStorageLogParam, 100> param_arr;
  int64_t offset[100];
  offset[0] = dummy_header_.get_serialize_size();
  SimpleObSlog slog_arr[100];

  for (int i = 0; i < 100; i++) {
    data_len[i] = ObRandom::rand(1<<10, 10<<10);
    content[i] = (char) ObRandom::rand(10, 100);
    slog_arr[i].set(data_len[i], content[i]);
    log_param.cmd_ = 35;
    log_param.data_ = &(slog_arr[i]);
    param_arr.push_back(log_param);
    if (0 != i) {
      offset[i] = offset[i-1] + dummy_entry_.get_serialize_size() + data_len[i-1];
    }
  }
  ret = slogger_->write_log(param_arr);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < 100; i++) {
    disk_addr = param_arr.at(i).disk_addr_;
    ASSERT_EQ(offset[i], disk_addr.offset_);
    ASSERT_EQ(data_len[i] + dummy_entry_.get_serialize_size(), disk_addr.size_);
    ASSERT_EQ(2, disk_addr.file_id_);
  }
  slogger_->get_active_cursor(output_cursor);
  ASSERT_EQ(109, output_cursor.log_id_);
  ASSERT_EQ(offset[99] + dummy_entry_.get_serialize_size() + data_len[99],
      output_cursor.offset_);

  int64_t pos = 0;
  for (int i = 0; i < 100; i++) {
    disk_addr = param_arr.at(i).disk_addr_;
    buf = ob_malloc(disk_addr.size_, "ReadBuf");
    ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, disk_addr.size_, buf, pos, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    cmp = MEMCMP(slog_arr[i].buf_, (char *)buf + pos, data_len[i]);

    ASSERT_EQ(0, cmp);
    ob_free(buf);
    buf = nullptr;
  }
}

TEST_F(TestStorageLogRW, test_iter_read_switch_file)
{
  ObStorageLogParam log_param;
  log_param.cmd_ = 29;
  ObLogCursor read_cursor;
  read_cursor.file_id_ = 1;
  read_cursor.log_id_ = 1;
  read_cursor.offset_ = 0;

  // write five huge single-slogs
  SimpleObSlog huge_slog((32<<20) - (10<<10), 'p');
  log_param.data_ = &huge_slog;
  for (int i = 0; i < 5; i++) { // to reach the max size of file
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(log_param));
  }

  // write four huge batch_slogs
  for (int i = 0; i < 4; i++) {
    ObSEArray<ObStorageLogParam, 8> param_arr;
    SimpleObSlog slog_arr[8];
    for (int j = 0; j < 8; j++) {
      slog_arr[i].set((4<<20) - (4<<10), 'g');
      log_param.data_ = &(slog_arr[i]);
      param_arr.push_back(log_param);
    }
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(param_arr));
  }


  ObStorageLogReader reader;
  char *read_buf = nullptr;
  ObStorageLogEntry entry;
  ObMetaDiskAddr disk_addr;
  reader.init(slogger_->get_dir(), read_cursor, log_file_spec_, OB_SERVER_TENANT_ID);
  // iter read five huge single-slogs
  int index = 0;
  while (index < 5) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, disk_addr));
    if (29 == entry.cmd_) {
      ASSERT_EQ(index + 1, entry.seq_);
      ASSERT_EQ(0, MEMCMP(huge_slog.buf_, read_buf, (32<<20) - (10<<10)));
      index++;
    }
  }

  // iter read four huge batch-slogs
  SimpleObSlog tmp_slog((4<<20) - (4<<10), 'g');
  while (index < 37) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, disk_addr));
    if (29 == entry.cmd_) {
      ASSERT_EQ(index + 1, entry.seq_);
      ASSERT_EQ(0, MEMCMP(tmp_slog.buf_, read_buf, (4<<20) - (4<<10)));
      index++;
    }
  }

  // write normal single-slogs in file 2
  int64_t data_len[10];
  char content[10];
  SimpleObSlog slog_arr[10];
  ObSEArray<ObStorageLogParam, 10> param_arr;
  for (int i = 0; i < 10; i++) {
    data_len[i] = ObRandom::rand(1<<10, 20<<10);
    content[i] = (char) ObRandom::rand(30, 100);
    slog_arr[i].set(data_len[i], content[i]);
    log_param.data_ = &(slog_arr[i]);
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(log_param));
    param_arr.push_back(log_param);
  }

  // read normal single-slogs
  index = 0;
  while (index < 10) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, disk_addr));
    if (29 == entry.cmd_) {
      ASSERT_EQ(index + 38, entry.seq_);
      ASSERT_EQ(0, MEMCMP(slog_arr[index].buf_, read_buf, data_len[index]));
      ASSERT_EQ(param_arr[index].disk_addr_, disk_addr);
      ASSERT_EQ(2, disk_addr.file_id_);
      index++;
    }
  }
}

TEST_F(TestStorageLogRW, large_num_slogs)
{
  int ret = OB_SUCCESS;

  ObStorageLogParam log_param;
  log_param.cmd_ = 29;
  ObLogCursor read_cursor;
  read_cursor.file_id_ = 1;
  read_cursor.log_id_ = 1;
  read_cursor.offset_ = 0;

  ObStorageLogReader reader;
  char *read_buf = nullptr;
  ObStorageLogEntry entry;
  ObMetaDiskAddr disk_addr;
  reader.init(slogger_->get_dir(), read_cursor, log_file_spec_, OB_SERVER_TENANT_ID);

  // write large number of normal batch-slogs
  int64_t batch_data_len[20];
  int64_t batch_content[20];
  int64_t batch_num = 20;
  SimpleObSlog batch_slog_arr[20];
  ObSEArray<ObStorageLogParam, 20> batch_param_arr;
  for (int64_t i = 0; i < 20; i++) {
    batch_data_len[i] = ObRandom::rand(1<<10, 10<<10);
    batch_content[i] = (char) ObRandom::rand(30, 100);
    batch_slog_arr[i].set(batch_data_len[i], batch_content[i]);
    log_param.data_ = &(batch_slog_arr[i]);
    batch_param_arr.push_back(log_param);
  }
  for (int64_t i = 0; i < 500; i++) {
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(batch_param_arr));
  }

  // write large number of normal single-slogs
  int64_t data_len[500];
  char content[500];
  SimpleObSlog slog_arr[500];
  ObSEArray<ObStorageLogParam, 500> param_arr;
  for (int64_t i = 0; i < 500; i++) {
    data_len[i] = ObRandom::rand(1<<10, 100<<10);
    content[i] = (char) ObRandom::rand(30, 100);
    slog_arr[i].set(data_len[i], content[i]);
    log_param.data_ = &(slog_arr[i]);
    ASSERT_EQ(OB_SUCCESS, slogger_->write_log(log_param));
    param_arr.push_back(log_param);
  }

  // read large number of normal batch-slogs
  for (int64_t i = 0; i < 500; i++) {
    int64_t index = 0;
    while(index < 20) {
      ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, disk_addr));
      if (29 == entry.cmd_) {
        ASSERT_EQ(20 * i + index + 1, entry.seq_);
        ASSERT_EQ(0, MEMCMP(batch_slog_arr[index].buf_, read_buf, batch_data_len[index]));
        index++;
      }
    }
  }

  // read large number of normal single-slogs
  int64_t index = 0;
  while(index < 500) {
    ASSERT_EQ(OB_SUCCESS, reader.read_log(entry, read_buf, disk_addr));
    if (29 == entry.cmd_) {
      ASSERT_EQ(index + 1, entry.seq_ - 10000);
      ASSERT_EQ(0, MEMCMP(slog_arr[index].buf_, read_buf, data_len[index]));
      ASSERT_EQ(param_arr[index].disk_addr_, disk_addr);
      index++;
    }
  }
}

TEST_F(TestStorageLogRW, test_exlarge_count_slogs)
{
  int ret = OB_SUCCESS;
  SimpleObSlog simple_slog(1, 's');
  int64_t total_cnt = 50000;
  ObSEArray<ObStorageLogParam, 20> param_arr;

  ASSERT_EQ(common::OB_SUCCESS, param_arr.reserve(total_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
    ObStorageLogParam log_param;
    log_param.cmd_ = 3;
    log_param.data_ = &simple_slog;
    ASSERT_EQ(common::OB_SUCCESS, param_arr.push_back(log_param));
  }
  ASSERT_EQ(OB_SIZE_OVERFLOW, slogger_->write_log(param_arr));

  total_cnt /= 2;
  param_arr.reset();
  ASSERT_EQ(common::OB_SUCCESS, param_arr.reserve(total_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
    ObStorageLogParam log_param;
    log_param.cmd_ = 8;
    log_param.data_ = &simple_slog;
    ASSERT_EQ(common::OB_SUCCESS, param_arr.push_back(log_param));
  }
  ASSERT_EQ(OB_SUCCESS, slogger_->write_log(param_arr));
}

TEST_F(TestStorageLogRW, test_multiple_threads)
{
  int ret = OB_SUCCESS;

  ObStorageLogParam log_param1;
  log_param1.cmd_ = 3;
  SimpleObSlog simple_slog1(512<<10, 'I');
  log_param1.data_ = &simple_slog1;

  ObStorageLogParam log_param2;
  log_param2.cmd_ = 77;
  SimpleObSlog simple_slog2(1, 'I');
  log_param2.data_ = &simple_slog2;

  ObStorageLogParam log_param_ele;
  log_param_ele.cmd_ = 79;

  ObSEArray<ObStorageLogParam, 20> param_arr1;
  int64_t data_len1[20];
  SimpleObSlog slog_arr1[20];
  for (int i = 0; i < 20; i++) {
    data_len1[i] = ObRandom::rand(1, 10<<10);
    char content = (char) ObRandom::rand(10, 100);
    slog_arr1[i].set(data_len1[i], content);
    log_param_ele.data_ = &(slog_arr1[i]);
    param_arr1.push_back(log_param_ele);
  }

  ObSEArray<ObStorageLogParam, 100> param_arr2;
  int64_t data_len2[100];
  SimpleObSlog slog_arr2[100];
  for (int i = 0; i < 100; i++) {
    data_len2[i] = ObRandom::rand(10<<10, 20<<10);
    char content = (char) ObRandom::rand(10, 100);
    slog_arr2[i].set(data_len2[i], content);
    log_param_ele.data_ = &(slog_arr2[i]);
    param_arr2.push_back(log_param_ele);
  }

  using single_write = int(ObStorageLogger::*)(ObStorageLogParam &);
  using batch_write = int(ObStorageLogger::*)(ObIArray<ObStorageLogParam> &);

  std::thread t1(std::bind((single_write)&ObStorageLogger::write_log, slogger_, std::ref(log_param1)));
  std::thread t2(std::bind((single_write)&ObStorageLogger::write_log, slogger_, std::ref(log_param2)));
  std::thread t3(std::bind((batch_write)&ObStorageLogger::write_log, slogger_, std::ref(param_arr1)));
  std::thread t4(std::bind((batch_write)&ObStorageLogger::write_log, slogger_, std::ref(param_arr2)));

  t1.join();
  t2.join();
  t3.join();
  t4.join();

  ASSERT_EQ(123, slogger_->log_seq_);
  ASSERT_EQ(123, slogger_->log_writer_->cursor_.log_id_);

  void *buf = nullptr;
  buf = ob_malloc(516<<10, "ReadBuf");
  ObMetaDiskAddr disk_addr;
  int cmp = 0;
  int64_t pos = 0;

  disk_addr = log_param1.disk_addr_;
  ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, 516<<10, buf, pos, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = MEMCMP(simple_slog1.buf_, (char *)buf + pos, 512<<10);
  ASSERT_EQ(0, cmp);

  disk_addr = log_param2.disk_addr_;
  ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, 516<<10, buf, pos, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = MEMCMP(simple_slog2.buf_, (char *)buf + pos, 1);
  ASSERT_EQ(0, cmp);

  for (int i = 0; i < 20; i++) {
    disk_addr = param_arr1.at(i).disk_addr_;
    ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, 516<<10, buf, pos, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    cmp = MEMCMP(slog_arr1[i].buf_, (char *)buf + pos, data_len1[i]);
    ASSERT_EQ(0, cmp);
  }

  for (int i = 0; i < 100; i++) {
    disk_addr = param_arr2.at(i).disk_addr_;
    ret = ObStorageLogReader::read_log(slogger_->get_dir(), disk_addr, 516<<10, buf, pos, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    cmp = MEMCMP(slog_arr2[i].buf_, (char *)buf + pos, data_len2[i]);
    ASSERT_EQ(0, cmp);
  }

  ob_free(buf);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_log_read_write.log*");
  OB_LOGGER.set_file_name("test_storage_log_read_write.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
