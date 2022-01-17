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

#include "clog/ob_clog_writer.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_info_block_handler.h"
#include "clog/ob_log_common.h"
#include "clog/ob_log_type.h"
#include "clog/ob_log_cache.h"
#include "clog/ob_log_engine.h"
#include "clog/ob_clog_file_writer.h"
#include "clog/ob_log_block.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/resource/achunk_mgr.h"
#include "lib/random/ob_random.h"
#include "share/ob_proposal_id.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_tenant_mgr.h"
#include "observer/ob_server_struct.h"
#include "share/redolog/ob_log_file_reader.h"

#include <libaio.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::lib;

namespace oceanbase {
namespace unittest {
class MyMetaInfoGenerator : public ObIInfoBlockHandler {
public:
  MyMetaInfoGenerator()
  {}
  virtual ~MyMetaInfoGenerator()
  {}
  virtual int build_info_block(char* buf, const int64_t buf_len, int64_t& pos);
  virtual int resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int update_info(const int64_t max_submit_timestamp);
  virtual int64_t get_entry_cnt() const;
};

// Mock a info block whose content is all 'Z' character, and its length variant from 2M to 512
int MyMetaInfoGenerator::build_info_block(char* buf, const int64_t buf_len, int64_t& pos)
{
  UNUSED(buf_len);
  memset(buf, 'Z', buf_len);
  pos = 1024 * 1024 + 512 * 1024;
  return OB_SUCCESS;
}

int MyMetaInfoGenerator::resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int MyMetaInfoGenerator::update_info(const int64_t max_submit_timestamp)
{
  UNUSED(max_submit_timestamp);
  return OB_SUCCESS;
}

int64_t MyMetaInfoGenerator::get_entry_cnt() const
{
  return 1;
}

class MyCLogItem : public ObICLogItem {
public:
  MyCLogItem() : buf_(NULL), data_len_(0), is_flushed_(false), file_id_(0), offset_(0), err_code_(0)
  {
    cond_.init(1);
  }
  virtual ~MyCLogItem()
  {}
  virtual bool is_valid() const
  {
    return NULL != buf_ && data_len_ > 0;
  }
  virtual char* get_buf()
  {
    return buf_;
  }
  virtual const char* get_buf() const
  {
    return buf_;
  }
  virtual int64_t get_data_len() const
  {
    return data_len_;
  }
  virtual int after_flushed(
      const file_id_t file_id, const offset_t offset, const int error_code, const ObLogWritePoolType type);
  void wait();
  TO_STRING_KV(KP_(buf), K_(data_len));
  char* buf_;
  int64_t data_len_;
  bool is_flushed_;
  file_id_t file_id_;
  offset_t offset_;
  int err_code_;
  ObThreadCond cond_;
};

int MyCLogItem::after_flushed(
    const file_id_t file_id, const offset_t offset, const int error_code, const ObLogWritePoolType type)
{
  UNUSED(type);
  ObThreadCondGuard guard(cond_);
  file_id_ = file_id;
  offset_ = offset;
  err_code_ = error_code;
  is_flushed_ = true;
  cond_.signal();
  return OB_SUCCESS;
}

void MyCLogItem::wait()
{
  while (!is_flushed_) {
    ObThreadCondGuard guard(cond_);
    if (!is_flushed_) {
      cond_.wait();
    }
  }
}

class TestCLogWriter : public ::testing::Test {
public:
  TestCLogWriter();
  virtual ~TestCLogWriter();
  virtual void SetUp();
  virtual void TearDown();

protected:
  char log_path_[1024];
  char* log_buf_;
  ObLogDir log_dir_;
  ObLogWriteFilePool write_pool_;
  ObLogFileStore file_store_;
  ObCLogLocalFileWriter log_file_writer_;
  ObLogCache log_cache_;
  MyMetaInfoGenerator info_getter_;
  ObTailCursor tail_cursor_;
  ObCLogWriter clog_writer_;
  ObCLogWriterCfg clog_cfg_;
};

TestCLogWriter::TestCLogWriter()
{
  getcwd(log_path_, 1024);
  strcat(log_path_, "/test_clog");
  clog_cfg_.log_file_writer_ = &log_file_writer_;
  clog_cfg_.log_cache_ = &log_cache_;
  clog_cfg_.tail_ptr_ = &tail_cursor_;
  clog_cfg_.info_getter_ = &info_getter_;
  clog_cfg_.type_ = CLOG_WRITE_POOL;
  clog_cfg_.base_cfg_.group_commit_max_item_cnt_ = 1;
  clog_cfg_.base_cfg_.group_commit_min_item_cnt_ = 1;
  clog_cfg_.base_cfg_.group_commit_max_wait_us_ = 100;
  log_buf_ = (char*)malloc(2 * 1024 * 1024);
}

TestCLogWriter::~TestCLogWriter()
{
  free(log_buf_);
}

void TestCLogWriter::SetUp()
{
  int ret = OB_SUCCESS;
  TearDown();

  common::ObAddr addr(ObAddr::VER::IPV4, "100.81.152.48", 2828);

  GCTX.self_addr_ = addr;
  system("rm -rf ./test_clog");
  system("mkdir test_clog");
  const int64_t hot_cache_size = 1L << 27;
  ret = log_cache_.init(addr, "clog_cache", 1, hot_cache_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObHotCacheWarmUpHelper::test_warm_up(log_cache_);
  ret = log_dir_.init(log_path_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_pool_.init(&log_dir_, CLOG_FILE_SIZE, clog_cfg_.type_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = file_store_.init(log_path_, CLOG_FILE_SIZE, clog_cfg_.type_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_file_writer_.init(log_path_, CLOG_DIO_ALIGN_SIZE, &file_store_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = clog_writer_.init(clog_cfg_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_LOG_FILE_READER.init();
}

void TestCLogWriter::TearDown()
{
  log_file_writer_.destroy();
  file_store_.destroy();
  clog_writer_.destroy();
  write_pool_.destroy();
  OB_CLOG_DISK_MGR.destroy();
  log_cache_.destroy();
  ObBaseLogBufferMgr::get_instance().destroy();
  system("rm -rf ./test_clog");
}

TEST_F(TestCLogWriter, normal)
{
  int ret = OB_SUCCESS;
  file_id_t file_id = 1;
  offset_t offset = 0;
  ObCLogWriterCfg log_cfg;
  // double destroy
  clog_writer_.destroy();
  clog_writer_.destroy();

  // invalid init
  ret = clog_writer_.init(log_cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid invoke when not init
  ret = clog_writer_.start(file_id, offset);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = clog_writer_.switch_file();
  ASSERT_NE(OB_SUCCESS, ret);

  // normal init
  ret = clog_writer_.init(clog_cfg_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = clog_writer_.init(clog_cfg_);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid start argument
  file_id = 0;
  offset = 0;
  ret = clog_writer_.start(file_id, offset);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal start
  file_id = 1;
  offset = 0;
  ret = clog_writer_.start(file_id, offset);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat start
  file_id = 1;
  offset = 0;
  ret = clog_writer_.start(file_id, offset);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid log item
  // MyCLogItem log_item;
  // ret = clog_writer_.append_log(log_item);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // log_item.wait();
  // ASSERT_NE(OB_SUCCESS, log_item.err_code_);

  clog_writer_.destroy();
  clog_writer_.destroy();
}

TEST_F(TestCLogWriter, border)
{
  int ret = OB_SUCCESS;
  file_id_t file_id = 1;
  offset_t offset = 0;
  MyCLogItem log_item;
  ObLogBlockMetaV2 block;
  const int64_t block_meta_size = block.get_serialize_size();
  log_item.buf_ = log_buf_ + block_meta_size;
  log_item.data_len_ = 0;

  // normal start
  ret = clog_writer_.start(file_id, offset);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObPartitionKey partition_key(1, 3001, 1);

  // normal write
  while (true) {
    if (offset > CLOG_MAX_DATA_OFFSET - 2 * 1024 * 1024) {
      log_item.data_len_ = 1024 * 1024 + 512 * 1024;
    } else {
      log_item.data_len_ = ObRandom::rand(1024, 1024 * 1024);
    }

    log_item.is_flushed_ = false;
    memset(log_item.buf_, (uint8_t)ObRandom::rand(100, 132), log_item.data_len_);

    ObLogEntryHeader header;
    common::ObProposalID rts;
    rts.ts_ = ObTimeUtility::fast_current_time();
    int64_t pos = 0;
    const int64_t header_size = header.get_serialize_size();
    ret = header.generate_header(OB_LOG_SUBMIT, partition_key,
                                 1, log_item.buf_ + header_size, log_item.data_len_ - header_size,
                                 ObTimeUtility::fast_current_time(),
                                 ObTimeUtility::fast_current_time(),
                                 rts,
                                 ObTimeUtility::fast_current_time(),
                                 ObVersion(0), true);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ret = header.serialize(log_item.buf_, log_item.data_len_, pos);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ASSERT_EQ(header_size, pos);

    CLOG_LOG(INFO, "flush one item start", K(file_id), K(offset), K(log_item.data_len_));

    ret = clog_writer_.append_log(log_item);
    ASSERT_EQ(OB_SUCCESS, ret);
    log_item.wait();
    ASSERT_EQ(OB_SUCCESS, log_item.err_code_);
    if (file_id == log_item.file_id_) {
      if (offset + static_cast<offset_t>(block_meta_size) != log_item.offset_) {
        CLOG_LOG(ERROR, "Not equal offset, ", K(offset), K(log_item.offset_), K(file_id));
      }
      ASSERT_EQ(offset + static_cast<offset_t>(block_meta_size), log_item.offset_);
      offset += (uint32_t)log_item.data_len_ + static_cast<offset_t>(block_meta_size);
    } else {
      file_id = log_item.file_id_;
      ASSERT_EQ(static_cast<offset_t>(block_meta_size), log_item.offset_);
      offset = (uint32_t)log_item.data_len_ + static_cast<offset_t>(block_meta_size);
    }

    CLOG_LOG(INFO, "flush one item end", K(offset), K(log_item.offset_), K(log_item.data_len_), K(file_id));

    if (file_id > 3) {
      break;
    }
  }

  clog_writer_.destroy();
  log_file_writer_.reset();

  int64_t test_lower = lower_align(4096, 4096);
  int64_t test_upper = upper_align(4096, 4096);
  CLOG_LOG(INFO, "cooper", K(test_lower), K(test_upper));
  ASSERT_EQ(4096, test_lower);
  ASSERT_EQ(4096, test_upper);

  //try locate last time write position
  ObLogDirectReader log_reader;
  ObLogCache log_cache;
  ObTailCursor tail_cursor;
  file_id_t restart_file_id = 0;
  offset_t restart_file_id_offset = 0;

  ret = log_reader.init(log_path_, nullptr, true, &log_cache_, &tail_cursor, ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = locate_clog_tail(1000000000, &file_store_, &log_reader, restart_file_id, restart_file_id_offset);
  ASSERT_EQ(OB_SUCCESS, ret);

  offset = upper_align(offset, CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(file_id, restart_file_id);
  ASSERT_EQ(offset, restart_file_id_offset);

  // continue write clog
  ret = clog_writer_.init(clog_cfg_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = clog_writer_.start(restart_file_id, restart_file_id_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  log_item.data_len_ = ObRandom::rand(1, 1024 * 1024);
  log_item.is_flushed_ = false;
  memset(log_item.buf_, (uint8_t)ObRandom::rand(0, 127), log_item.data_len_);
  ret = clog_writer_.append_log(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  log_item.wait();
  ASSERT_EQ(OB_SUCCESS, log_item.err_code_);
  ASSERT_EQ(offset + static_cast<offset_t>(block_meta_size), log_item.offset_);

  log_reader.destroy();
  log_cache.destroy();
}

TEST_F(TestCLogWriter, errsim_aio_timeout)
{
  int ret = OB_SUCCESS;
  file_id_t file_id = 1;
  offset_t offset = 0;
  MyCLogItem log_item;
  ObLogBlockMetaV2 block;

  // prepare log item
  const int64_t block_meta_size = block.get_serialize_size();
  log_item.buf_ = log_buf_ + block_meta_size;
  log_item.is_flushed_ = false;
  log_item.data_len_ = ObRandom::rand(1, 1024 * 1024);
  memset(log_item.buf_, (uint8_t)ObRandom::rand(100, 132), log_item.data_len_);

  // normal start
  ret = clog_writer_.start(file_id, offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  bool is_disk_error = clog_writer_.is_disk_error();
  ASSERT_FALSE(is_disk_error);

  // write log item
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_AIO_TIMEOUT, 0, 1);
  usleep(100 * 1000);
#endif
  ret = clog_writer_.append_log(log_item);
  ASSERT_EQ(OB_SUCCESS, ret);

  // let IO finish
  sleep(1);
  is_disk_error = clog_writer_.is_disk_error();

#ifdef ERRSIM
  ASSERT_TRUE(is_disk_error);
#else
  ASSERT_FALSE(is_disk_error);
#endif

  clog_writer_.destroy();
  log_file_writer_.reset();

#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_AIO_TIMEOUT, 0, 0);
  usleep(100 * 1000);
#endif
}
} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_clog_writer.log*");
  OB_LOGGER.set_file_name("test_clog_writer.log", true);
  OB_LOGGER.set_log_level("DEBUG");

  ObTenantManager::get_instance().init(100000);
  ObTenantManager::get_instance().add_tenant(OB_SYS_TENANT_ID);
  ObTenantManager::get_instance().set_tenant_mem_limit(
      OB_SYS_TENANT_ID, 10L * 1024L * 1024L * 1024L, 10L * 1024L * 1024L * 1024L);
  ObKVGlobalCache::get_instance().init();
  // set observer memory limit
  CHUNK_MGR.set_limit(15L * 1024L * 1024L * 1024L);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
