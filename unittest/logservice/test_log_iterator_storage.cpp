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

#include "unittest/logservice/test_shared_log_common.h"
#define private public
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_log_iterator_storage.h"
#endif
#undef private

namespace oceanbase
{
// 单元测试中，将每个文件以及每条日志设置的小一些，可以在有限的时间内跑到更多的场景
const int64_t default_phy_block_size = 16 * 1024;
const int64_t default_block_size = default_phy_block_size - palf::MAX_INFO_BLOCK_SIZE;
// 每个文件中存在8K/256(32)条日志
const int64_t single_log_entry_size = 256;
const int64_t log_entrys_in_single_block = default_block_size / single_log_entry_size;
const int64_t max_log_buffer_size = 2 * single_log_entry_size;
const int64_t iterator_single_read_size = max_log_buffer_size;
int64_t logservice::ObLogSharedStorage::BLOCK_SIZE   = default_block_size;
int64_t logservice::ObLogSharedStorage::PHY_BLOCK_SIZE   = default_phy_block_size;
int64_t logservice::ObLogSharedStorage::MEMORY_LIMIT = default_block_size;
int64_t logservice::ObLogSharedStorage::READ_SIZE    = default_block_size/5;
int64_t logservice::ObLogSharedStorage::MAX_LOG_SIZE = max_log_buffer_size;

namespace unittest
{
using namespace share;
using namespace common;
using namespace logservice;
using namespace palf;

typedef PalfIterator<LogEntry> S2Iterator;
S2Iterator iterator;

const std::string TestLogExternalStorageCommom::BASE_DIR = "test_log_iterator_storage_dir";

class TestLogIteratorStorage : public TestLogExternalStorageCommom {
public:
private:
virtual int upload_block_impl_(const uint64_t tenant_id,
                               const int64_t palf_id,
                               const palf::block_id_t block_id,
                               const SCN &scn)
{
  int ret = OB_SUCCESS;
  LogBlockHeader log_block_header;
  char *write_buf = reinterpret_cast<char*>(mtl_malloc(default_phy_block_size, "test"));
  if (NULL == write_buf){
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  memset(write_buf, 'c', default_phy_block_size);
  LSN lsn(block_id*default_block_size);
  int64_t pos = 0;
  log_block_header.update_lsn_and_scn(lsn, scn);
  log_block_header.calc_checksum();
  if (OB_FAIL(log_block_header.serialize(write_buf, default_phy_block_size, pos))) {
    CLOG_LOG(ERROR, "serialize failed", K(pos));
  } else if (FALSE_IT(pos = MAX_INFO_BLOCK_SIZE)) {
  } else {
    SCN click = SCN::base_scn();
    SCN tmp_scn = scn;
    while (pos < default_phy_block_size) {
      LogEntryHeader header;
      bool need_generate_padding = false;
      const int64_t random_log_size = random() % (single_log_entry_size - header.get_serialize_size())
                                       + 1 + header.get_serialize_size();
      if (random_log_size + pos > default_phy_block_size - single_log_entry_size) {
        need_generate_padding = true;
        CLOG_LOG(INFO, "need_generate_padding", K(pos), K(random_log_size), K(single_log_entry_size),
                 K(default_phy_block_size));
      }
      const int64_t curr_log_size = need_generate_padding ? default_phy_block_size - pos : random_log_size;
      // const int64_t curr_log_size = single_log_entry_size;
      if (OB_FAIL(header.generate_header(write_buf + pos + header.get_serialize_size(),
                                         curr_log_size - header.get_serialize_size(),
                                         tmp_scn))) {
        CLOG_LOG(ERROR, "generate_header failed");
      } else if (OB_FAIL(header.serialize(write_buf, default_phy_block_size, pos))) {
        CLOG_LOG(ERROR, "serilize failed");
      } else {
        pos += (curr_log_size-header.get_serialize_size());
        tmp_scn.scn_inc(click);
        CLOG_LOG(INFO, "serialize log entry success", K(tenant_id), K(palf_id), K(block_id),
                 K(log_block_header), K(pos));
      }
    }

    if (OB_SUCC(ret)
        && OB_FAIL(GLOBAL_EXT_HANDLER.upload(tenant_id, palf_id, block_id, write_buf, default_phy_block_size))) {
      CLOG_LOG(ERROR, "upload failed", K(tenant_id), K(palf_id), K(block_id));
    }
  }
  if (NULL != write_buf) {
    mtl_free(write_buf);
  }
  return ret;
}
};

class LocalLogStorage : public palf::PalfHandleImpl {
public:
  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_UNEXPECTED, file maybe deleted by human.
  int raw_read(const LSN &lsn,
               char *read_buf,
               const int64_t in_read_size,
               int64_t &out_read_size,
               palf::LogIOContext &io_ctx) final
  {
    int ret = OB_SUCCESS;
    if (!lsn.is_valid() || 0 >= in_read_size || NULL == read_buf) {
      ret = OB_INVALID_ARGUMENT;
    } else if (lsn < begin_lsn_) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    } else if (lsn >= log_tail_) {
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    } else {
      const block_id_t read_block_id = lsn_2_block(lsn, default_block_size);
      const LSN curr_block_end_lsn = LSN((read_block_id + 1) * default_block_size);
      const LSN &max_readable_lsn = MIN(log_tail_, curr_block_end_lsn);
      const int64_t real_in_read_size = MIN(max_readable_lsn - lsn, in_read_size);
      const offset_t real_read_offset = lsn_2_offset(lsn, default_block_size) + MAX_INFO_BLOCK_SIZE;
      if (OB_FAIL(ext_handler_.pread(
        tenant_id_,
        palf_id_,
        read_block_id,
        real_read_offset,
        read_buf,
        real_in_read_size,
        out_read_size,
        io_ctx))) {
        PALF_LOG(
            WARN, "pread failed", K(ret), K(lsn), K(log_tail_), K(real_in_read_size));
      } else {
        PALF_LOG(TRACE,
                 "inner_pread success",
                 K(ret),
                 K(lsn),
                 K(in_read_size),
                 K(real_in_read_size),
                 K(lsn),
                 K(out_read_size),
                 K(log_tail_));
      }
    }
    return ret;
  }
  int init(const uint64_t tenant_id,
           const int64_t palf_id,
           const LSN &start_lsn,
           const LSN &end_lsn)
  {
    begin_lsn_ = start_lsn;
    log_tail_ = end_lsn;
    tenant_id_ = tenant_id;
    palf_id_ = palf_id;
    ext_handler_.init();
    return ext_handler_.start(0);
  }
  void set_start_lsn(const LSN &start_lsn) { begin_lsn_ = start_lsn; }
  void set_end_lsn(const LSN &end_lsn) { log_tail_ = end_lsn; }
  TO_STRING_KV(K_(tenant_id), K_(palf_id), K_(begin_lsn), K_(log_tail));
private:
  ObLogExternalStorageHandler ext_handler_;
  uint64_t tenant_id_;
  int64_t palf_id_;
  LSN begin_lsn_;
  LSN log_tail_;
};
int64_t global_palf_id = 0;

TEST_F(TestLogIteratorStorage, test_memory_storage)
{
  CLOG_LOG(INFO, "begin test_memory_storage");
  palf::LogIOContext io_ctx(palf::LogIOUser::OTHER);
  // case1: 测试init destroy接口
  {
    CLOG_LOG(INFO, "begin case1");
    ObLogMemoryStorage tmp_mem_storage;
    LSN invalid_lsn;
    LSN valid_lsn(PALF_INITIAL_LSN_VAL);
    // OB_INVALID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.init(invalid_lsn));

    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.init(valid_lsn));

    // OB_INIT_TWICE
    EXPECT_EQ(OB_INIT_TWICE, tmp_mem_storage.init(valid_lsn));

    EXPECT_EQ(true, tmp_mem_storage.is_inited_);
    tmp_mem_storage.destroy();

    EXPECT_EQ(false, tmp_mem_storage.is_inited_);
  }

  // case2: 测试append接口
  {
    CLOG_LOG(INFO, "begin case2");
    ObLogMemoryStorage tmp_mem_storage;
    LSN valid_lsn(PALF_INITIAL_LSN_VAL);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.init(valid_lsn));
    char buf[MAX_INFO_BLOCK_SIZE] = {'c'};

    // OB_INVALID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.append(LSN(), NULL, 0));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.append(valid_lsn, NULL, 0));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.append(valid_lsn, buf, 0));

    // OB_ERR_UNEXPECTED, lsn和log_tail不连续
    EXPECT_EQ(OB_ERR_UNEXPECTED, tmp_mem_storage.append(LSN(10), buf, MAX_INFO_BLOCK_SIZE));

    // OB_SUCCESS
    LSN init_lsn(0);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.append(init_lsn, buf, MAX_INFO_BLOCK_SIZE));
    EXPECT_EQ(tmp_mem_storage.start_lsn_, init_lsn);
    EXPECT_EQ(tmp_mem_storage.log_tail_, init_lsn + MAX_INFO_BLOCK_SIZE);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.append(LSN(10), buf, MAX_INFO_BLOCK_SIZE));
    EXPECT_EQ(tmp_mem_storage.start_lsn_, LSN(10));
    EXPECT_EQ(tmp_mem_storage.log_tail_, LSN(10) + MAX_INFO_BLOCK_SIZE);
  }

  // case3: 验证pread接口
  {
    CLOG_LOG(INFO, "begin case3");
    ObLogMemoryStorage tmp_mem_storage;
    LSN init_lsn(10);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.init(init_lsn));
    char buf[MAX_INFO_BLOCK_SIZE] = {'c'};
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.append(init_lsn, buf, MAX_INFO_BLOCK_SIZE));
    EXPECT_EQ(tmp_mem_storage.start_lsn_, init_lsn);
    EXPECT_EQ(tmp_mem_storage.log_tail_, init_lsn + MAX_INFO_BLOCK_SIZE);
    ReadBufGuard read_buf_guard("unittest", MAX_INFO_BLOCK_SIZE);
    // OB_INVALID_ARGUMENT
    ReadBuf invalid_read_buf(NULL, 0);
    ReadBuf &valid_read_buf = read_buf_guard.read_buf_;
    LSN invalid_lsn;
    LSN valid_lsn(10);
    const int64_t invalid_in_read_size = 0;
    const int64_t valid_in_read_size = 10;
    int64_t out_read_size = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.pread(
      invalid_lsn, invalid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.pread(
      valid_lsn, invalid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.pread(
      invalid_lsn, valid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.pread(
      invalid_lsn, invalid_in_read_size, valid_read_buf, out_read_size, io_ctx));

    // OB_ERR_OUT_OF_LOWER_BOUND
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, tmp_mem_storage.pread(
      LSN(0), 1000, valid_read_buf, out_read_size, io_ctx));

    // OB_ERR_OUT_OF_UPPER_BOUND
    EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, tmp_mem_storage.pread(
      valid_lsn+100000000, 100000, valid_read_buf, out_read_size, io_ctx));

    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.pread(
      valid_lsn, 10, valid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(10, out_read_size);
    EXPECT_EQ(0, strncmp(valid_read_buf.buf_, buf, 10));
  }

  // case4: 验证get_data_len get_read_pos reuse接口
  {
    CLOG_LOG(INFO, "begin case4");
    ObLogMemoryStorage tmp_mem_storage;
    LSN init_lsn(10);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.init(init_lsn));
    char buf[MAX_INFO_BLOCK_SIZE] = {'c'};
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.append(init_lsn, buf, MAX_INFO_BLOCK_SIZE));

    int64_t data_len = 0;
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_data_len(data_len));
    EXPECT_EQ(data_len, MAX_INFO_BLOCK_SIZE);

    // OB_INVALID_ARGUMENT
    LSN invalid_lsn;
    int64_t read_pos = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_mem_storage.get_read_pos(invalid_lsn, read_pos));

    // OB_ERR_OUT_OF_UPPER_BOUND
    LSN upper_lsn(10*MAX_LOG_BUFFER_SIZE);
    EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, tmp_mem_storage.get_read_pos(upper_lsn, read_pos));

    // OB_ERR_OUT_OF_LOWER_BOUND
    LSN lower_lsn(PALF_INITIAL_LSN_VAL);
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, tmp_mem_storage.get_read_pos(lower_lsn, read_pos));

    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_read_pos(init_lsn, read_pos));
    EXPECT_EQ(read_pos, 0);
    LSN test_lsn = init_lsn + 10;
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_read_pos(test_lsn, read_pos));
    EXPECT_EQ(read_pos, 10);
    test_lsn = init_lsn + MAX_INFO_BLOCK_SIZE;
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_read_pos(test_lsn, read_pos));
    EXPECT_EQ(read_pos, MAX_INFO_BLOCK_SIZE);

    LSN reuse_lsn(MAX_LOG_BUFFER_SIZE);
    tmp_mem_storage.reuse(reuse_lsn);
    EXPECT_EQ(nullptr, tmp_mem_storage.buf_);
    EXPECT_EQ(0, tmp_mem_storage.buf_len_);
    EXPECT_EQ(reuse_lsn, tmp_mem_storage.start_lsn_);
    EXPECT_EQ(reuse_lsn, tmp_mem_storage.log_tail_);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_data_len(data_len));
    EXPECT_EQ(0, data_len);
    EXPECT_EQ(OB_SUCCESS, tmp_mem_storage.get_read_pos(reuse_lsn, read_pos));
    EXPECT_EQ(0, read_pos);
    EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, tmp_mem_storage.get_read_pos(reuse_lsn+1, read_pos));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, tmp_mem_storage.get_read_pos(reuse_lsn-1, read_pos));
  }
}

TEST_F(TestLogIteratorStorage, test_shared_storage)
{
  CLOG_LOG(INFO, "begin test_shared_storage");
  uint64_t tenant_id = 1001;
  int64_t palf_id = ++global_palf_id;
  ObLogSharedStorage shared_storage;
  ObLogExternalStorageHandler ext_handler;
  EXPECT_EQ(OB_SUCCESS, ext_handler.init());
  EXPECT_EQ(OB_SUCCESS, ext_handler.start(2));
  uint64_t invalid_tenant_id = 0;
  uint64_t valid_tenant_id = 1;
  uint64_t invalid_palf_id = 0;
  uint64_t valid_palf_id = 1;
  LSN invalid_start_lsn;
  LSN valid_start_lsn(1);
  palf::LogIOContext io_ctx(palf::LogIOUser::OTHER);
  // case1: 测试init接口
  {
    CLOG_LOG(INFO, "begin case1");
    int64_t suggessted_read_buf_size = MAX_LOG_BUFFER_SIZE;
    ObLogSharedStorage tmp_shared_storage;
    // OB_INVALID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(invalid_tenant_id, invalid_palf_id, invalid_start_lsn, suggessted_read_buf_size, NULL));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(valid_tenant_id, invalid_palf_id, invalid_start_lsn, suggessted_read_buf_size, &ext_handler));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(invalid_tenant_id, valid_palf_id, invalid_start_lsn, suggessted_read_buf_size, &ext_handler));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(invalid_tenant_id, invalid_palf_id, valid_start_lsn, suggessted_read_buf_size, &ext_handler));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(invalid_tenant_id, invalid_palf_id, invalid_start_lsn, suggessted_read_buf_size, NULL));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.init(invalid_tenant_id, invalid_palf_id, invalid_start_lsn, suggessted_read_buf_size - 1, NULL));

    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, tmp_shared_storage.init(valid_tenant_id, valid_palf_id, valid_start_lsn, suggessted_read_buf_size, &ext_handler));

    // OB_INIT_TWICE
    EXPECT_EQ(OB_INIT_TWICE, tmp_shared_storage.init(valid_tenant_id, valid_palf_id, valid_start_lsn, suggessted_read_buf_size, &ext_handler));
    EXPECT_EQ(true, tmp_shared_storage.is_inited_);
    EXPECT_EQ(valid_tenant_id, tmp_shared_storage.tenant_id_);
    EXPECT_EQ(valid_palf_id, tmp_shared_storage.palf_id_);
    EXPECT_EQ(true, tmp_shared_storage.large_buffer_pool_.inited_);
    EXPECT_EQ(false, tmp_shared_storage.read_buf_.is_valid());
    EXPECT_EQ(true, tmp_shared_storage.mem_storage_.is_inited_);
    EXPECT_NE(nullptr, tmp_shared_storage.ext_handler_);
    tmp_shared_storage.destroy();
    EXPECT_EQ(false, tmp_shared_storage.is_inited_);
  }

  // case2: 测试pread接口
  {
    const int64_t suggessted_read_buf_size = logservice::ObLogSharedStorage::READ_SIZE;
    LSN start_lsn(default_block_size);
    LSN end_lsn(default_block_size);
    CLOG_LOG(INFO, "begin case2");
    ObLogSharedStorage tmp_shared_storage;
    EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id));
    EXPECT_EQ(OB_SUCCESS, tmp_shared_storage.init(tenant_id, palf_id, start_lsn, suggessted_read_buf_size, &ext_handler));
    ReadBufGuard read_buf_guard("unittest", MAX_INFO_BLOCK_SIZE);
    ReadBuf &read_buf = read_buf_guard.read_buf_;
    int64_t out_read_size = 0;

    LSN invalid_lsn;
    LSN valid_lsn(0);
    int64_t invalid_read_size = 0;
    int64_t valid_read_size = 1;
    ReadBuf invalid_read_buf;
    // OB_INVALID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.pread(invalid_lsn, invalid_read_size,
                                                            invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.pread(valid_lsn, invalid_read_size,
                                                            invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.pread(invalid_lsn, valid_read_size,
                                                            invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_shared_storage.pread(invalid_lsn, invalid_read_size,
                                                            read_buf, out_read_size, io_ctx));

    // OB_NO_SUCH_FILE_OR_DIRECTORY
    EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, tmp_shared_storage.pread(start_lsn, MAX_INFO_BLOCK_SIZE,
                                                                     read_buf, out_read_size, io_ctx));
    // OB_SUCCESS
    int64_t start_block_id = 0;
    EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id, start_block_id, 2));

    CLOG_LOG(INFO, "runlin trace", K(start_lsn));

    EXPECT_EQ(OB_SUCCESS, tmp_shared_storage.pread(start_lsn, single_log_entry_size,
                                                   read_buf, out_read_size, io_ctx));
    EXPECT_EQ(single_log_entry_size, out_read_size);
    char expected_data[MAX_INFO_BLOCK_SIZE];
    memset(expected_data, 'c', MAX_INFO_BLOCK_SIZE);
    LogEntry entry;
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, entry.deserialize(read_buf.buf_, out_read_size, pos));
    EXPECT_EQ(true, entry.header_.check_header_integrity());
  }

}

TEST_F(TestLogIteratorStorage, test_hybrid_storage)
{
  CLOG_LOG(INFO, "begin test_hybrid_storage");
  uint64_t tenant_id = 1001;
  int64_t palf_id_local = ++global_palf_id;
  int64_t palf_id_shared = ++global_palf_id;
  LocalLogStorage local_storage;
  ObLogSharedStorage shared_storage;
  char root_path[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  uint64_t storage_id = OB_INVALID_ID;
  const int64_t suggessted_read_buf_size = logservice::ObLogSharedStorage::READ_SIZE;
  palf::LogIOContext io_ctx(palf::LogIOUser::OTHER);

  block_id_t local_start_block_id = 10;
  block_id_t local_end_block_id = 15;
  block_id_t shared_start_block_id = 5;
  block_id_t shared_end_block_id = 12;

  // shared | local
  // 5-12   | 10-15
  LSN local_start_lsn(local_start_block_id * default_block_size);
  LSN local_end_lsn(local_end_block_id * default_block_size);
  LSN shared_start_lsn(shared_start_block_id * default_block_size);
  LSN shared_end_lsn(shared_end_block_id * default_block_size);

  ObLogExternalStorageHandler ext_handler;
  EXPECT_EQ(OB_SUCCESS, ext_handler.init());
  EXPECT_EQ(OB_SUCCESS, ext_handler.start(2));
  EXPECT_EQ(OB_SUCCESS, SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id));
  EXPECT_EQ(OB_SUCCESS, SHARED_LOG_GLOBAL_UTILS.construct_ls_str_(
    dest, tenant_id, ObLSID(palf_id_local), root_path, sizeof(root_path)));
  // local storage不依赖GLOBAL_UTILS
  EXPECT_EQ(OB_SUCCESS, local_storage.init(tenant_id, palf_id_local, local_start_lsn, local_end_lsn));
  // 本地存在10-15号文件
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id_local));
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id_local, local_start_block_id,
                                      local_end_block_id - local_start_block_id));

  // shared存在5-12号文件，此时mem_storage为空
  EXPECT_EQ(OB_SUCCESS, shared_storage.init(tenant_id, palf_id_shared, shared_start_lsn, logservice::ObLogSharedStorage::READ_SIZE, &ext_handler));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id_shared));
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id_shared, shared_start_block_id,
                                      shared_end_block_id - shared_start_block_id));

  ObLogHybridStorage hybrid_storage;
  // case1: 测试init接口
  {
    CLOG_LOG(INFO, "begin case1");
    EXPECT_EQ(OB_SUCCESS, hybrid_storage.init(tenant_id, palf_id_shared, shared_start_lsn, suggessted_read_buf_size, &ext_handler));
    hybrid_storage.local_storage_.palf_handle_guard_.palf_handle_.palf_handle_impl_ = &local_storage;
  }

  // case2: 测试pread接口
  {
    CLOG_LOG(INFO, "begin case2");
    LSN invalid_lsn;
    LSN valid_lsn(PALF_INITIAL_LSN_VAL);
    int64_t invalid_in_read_size = 0;
    int64_t valid_in_read_size = 1;
    ReadBuf invalid_read_buf;
    ReadBufGuard read_buf_guard("unittest", MAX_INFO_BLOCK_SIZE);
    ReadBuf &read_buf = read_buf_guard.read_buf_;
    int64_t out_read_size = 0;
    // OB_INVALID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, hybrid_storage.pread(invalid_lsn, invalid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, hybrid_storage.pread(valid_lsn, invalid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, hybrid_storage.pread(invalid_lsn, valid_in_read_size, invalid_read_buf, out_read_size, io_ctx));
    EXPECT_EQ(OB_INVALID_ARGUMENT, hybrid_storage.pread(invalid_lsn, invalid_in_read_size, read_buf, out_read_size, io_ctx));

    CLOG_LOG(INFO, "read out of lower bound");
    // OB_ERR_OUT_OF_LOWER_BOUND
    // 1. 读取local_storage，发现本地不存在所需数据;
    // 2. 读取shared_storage，发现shared上不存在所需数据，此步操作过后会reset shared_storage，避免内存hold
    LSN shared_lower_bound_lsn(PALF_INITIAL_LSN_VAL);
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, hybrid_storage.pread(
      shared_lower_bound_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));
    EXPECT_EQ(false, hybrid_storage.shared_storage_.read_buf_.is_valid());
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.start_lsn_);
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.log_tail_);

    CLOG_LOG(INFO, "read out of upper bound");
    // OB_ERR_OUT_OF_UPPER_BOUND
    LSN local_upper_bound_lsn(100000000000);
    EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, hybrid_storage.pread(
      local_upper_bound_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));

    CLOG_LOG(INFO, "read from local first");
    // 只会从local读，预期本地缓存无效
    LSN data_on_local_lsn(local_start_block_id*default_block_size);
    EXPECT_EQ(OB_SUCCESS, hybrid_storage.pread(
      data_on_local_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));
    EXPECT_EQ(false, hybrid_storage.shared_storage_.read_buf_.is_valid());
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.start_lsn_);
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.log_tail_);


    CLOG_LOG(INFO, "read from shared first");
    // local没有数据，预期从shared读
    LSN data_on_shared_lsn(shared_start_block_id*default_block_size);
    EXPECT_EQ(OB_SUCCESS, hybrid_storage.pread(
      data_on_shared_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));
    EXPECT_EQ(true, hybrid_storage.shared_storage_.read_buf_.is_valid());
    EXPECT_EQ(shared_start_lsn, hybrid_storage.shared_storage_.mem_storage_.start_lsn_);
    // 预期会读READ_SIZE大小
    EXPECT_EQ(shared_start_lsn + ObLogSharedStorage::READ_SIZE,
              hybrid_storage.shared_storage_.mem_storage_.log_tail_);

    CLOG_LOG(INFO, "read from local second");
    // local存在数据，预期从local读，并清空shared_storage
    EXPECT_EQ(OB_SUCCESS, hybrid_storage.pread(
      data_on_local_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));
    EXPECT_EQ(false, hybrid_storage.shared_storage_.read_buf_.is_valid());
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.start_lsn_);
    EXPECT_EQ(LSN(PALF_INITIAL_LSN_VAL), hybrid_storage.shared_storage_.mem_storage_.log_tail_);

    CLOG_LOG(INFO, "read from shared second");
    // local存在数据，预期从local读，并清空shared_storage
    data_on_shared_lsn = data_on_shared_lsn + MAX_INFO_BLOCK_SIZE;
    EXPECT_EQ(OB_SUCCESS, hybrid_storage.pread(
      data_on_shared_lsn, MAX_INFO_BLOCK_SIZE, read_buf, out_read_size, io_ctx));
  }
}

TEST_F(TestLogIteratorStorage, test_log_iterator)
{
  const int64_t suggessted_read_buf_size = logservice::ObLogSharedStorage::READ_SIZE;
  CLOG_LOG(INFO, "begin test_log_iterator");
  uint64_t tenant_id = 1001;
  int64_t palf_id_local = ++global_palf_id;
  int64_t palf_id_shared = ++global_palf_id;
  char root_path[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  uint64_t storage_id = OB_INVALID_ID;

  block_id_t local_start_block_id = 10;
  block_id_t local_end_block_id = 15;
  block_id_t shared_start_block_id = 5;
  block_id_t shared_end_block_id = 14;

  // shared | local
  // 5-14   | 10-15
  LSN local_start_lsn(local_start_block_id * default_block_size);
  LSN local_end_lsn(local_end_block_id * default_block_size);
  LSN shared_start_lsn(shared_start_block_id * default_block_size);
  LSN shared_end_lsn(shared_end_block_id * default_block_size);

  ObLogExternalStorageHandler ext_handler;
  EXPECT_EQ(OB_SUCCESS, ext_handler.init());
  EXPECT_EQ(OB_SUCCESS, ext_handler.start(2));
  EXPECT_EQ(OB_SUCCESS, SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id));
  EXPECT_EQ(OB_SUCCESS, SHARED_LOG_GLOBAL_UTILS.construct_ls_str_(
    dest, tenant_id, ObLSID(palf_id_local), root_path, sizeof(root_path)));
  LocalLogStorage local_storage;
  // local storage不依赖GLOBAL_UTILS
  EXPECT_EQ(OB_SUCCESS, local_storage.init(tenant_id, palf_id_local, local_start_lsn, local_end_lsn));
  // 本地存在10-15号文件
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id_local));
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id_local, local_start_block_id,
                                      local_end_block_id - local_start_block_id));

  // shared存在5-12号文件，此时mem_storage为空
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id_shared));
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id_shared, shared_start_block_id,
                                      shared_end_block_id - shared_start_block_id));

  ObLogHybridStorage hybrid_storage;
  hybrid_storage.type_ = ILogStorageType::DISK_STORAGE;
  ObLogSharedStorage &shared_storage = hybrid_storage.shared_storage_;;
  EXPECT_EQ(OB_SUCCESS, hybrid_storage.init(tenant_id, palf_id_shared, shared_start_lsn, suggessted_read_buf_size, &ext_handler));
  auto get_file_end_lsn = [&local_end_lsn] {
    return local_end_lsn;
  };
  hybrid_storage.local_storage_.palf_handle_guard_.palf_handle_.palf_handle_impl_ = &local_storage;

  // shared | local
  // 5-14   | 10-15
  // 验证正常读取操作，从shared 读到 local
  {
    CLOG_LOG(INFO, "begin case1");
    std::vector<LSN> local_start_lsns =
      {local_start_lsn, local_start_lsn + default_block_size, local_start_lsn + 2 * default_block_size};
    for (auto tmp_local_start_lsn : local_start_lsns) {
      S2Iterator s2_iterator;
      local_storage.set_start_lsn(tmp_local_start_lsn);
      EXPECT_EQ(OB_SUCCESS, s2_iterator.init(shared_start_lsn, get_file_end_lsn, &hybrid_storage));
      s2_iterator.iterator_impl_.next_round_pread_size_ = iterator_single_read_size;
      int ret = OB_SUCCESS;
      LogEntry entry;
      LSN lsn;
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, lsn));
        // 读到local_start_lsn会reset shared_storage
        if (lsn == tmp_local_start_lsn + iterator_single_read_size) {
          CLOG_LOG(INFO, "iterate tmp_local_end_lsn", K(lsn), K(tmp_local_start_lsn), K(iterator_single_read_size),
                   K(local_storage));
          EXPECT_EQ(nullptr, shared_storage.mem_storage_.buf_);
          EXPECT_EQ(0, shared_storage.mem_storage_.buf_len_);
        }
      }
      EXPECT_EQ(OB_ITER_END, ret);
      EXPECT_EQ(lsn + entry.get_serialize_size(), local_end_lsn);
    }
  }

  // 验证正常读取操作，local为空, 此时读到shared_end_lsn并返回OB_ERR_OUT_OF_LOWER_BOUND
  {
    hybrid_storage.local_storage_.palf_handle_guard_.palf_handle_.palf_handle_impl_ = NULL;
    CLOG_LOG(INFO, "begin case2");
    std::vector<LSN> shared_start_lsns =
      {shared_start_lsn, shared_start_lsn + default_block_size, shared_start_lsn + 2 * default_block_size};
    for (auto tmp_shared_start_lsn : shared_start_lsns) {
      S2Iterator s2_iterator;
      EXPECT_EQ(OB_SUCCESS, s2_iterator.init(tmp_shared_start_lsn, get_file_end_lsn, &hybrid_storage));
      s2_iterator.iterator_impl_.next_round_pread_size_ = iterator_single_read_size;
      int ret = OB_SUCCESS;
      LogEntry entry;
      LSN lsn;
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, lsn));
        // 读到local_start_lsn会reset shared_storage
      }
      EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ret);
      if (lsn.is_valid()) {
        EXPECT_EQ(lsn + entry.get_serialize_size(), shared_end_lsn);
      }
    }
    hybrid_storage.local_storage_.palf_handle_guard_.palf_handle_.palf_handle_impl_ = &local_storage;
  }

  // shared | local
  // 5-14   | 10-15
  // 验证读到一半local没有需要数据，转而向shared读取
  {
    CLOG_LOG(INFO, "begin case3");
    // shared | local
    // 5-14   | 10-15
    // read until 12.xx
    // 回收本地到14
    std::vector<LSN> read_until_log_start_lsns;
    for (int i = 1; i < log_entrys_in_single_block; i++) {
      LSN tmp_read_until_lsn = LSN(i*single_log_entry_size) + local_start_lsn.val_ + 2 * default_block_size;
      read_until_log_start_lsns.push_back(tmp_read_until_lsn);
    }
    for (auto tmp_read_until_log_start_lsn : read_until_log_start_lsns) {
      S2Iterator s2_iterator;
      // 将iterator的单次读磁盘大小变小
      EXPECT_EQ(OB_SUCCESS, s2_iterator.init(local_start_lsn, get_file_end_lsn, &hybrid_storage));
      s2_iterator.iterator_impl_.next_round_pread_size_ = iterator_single_read_size;
      int ret = OB_SUCCESS;
      LogEntry entry;
      LSN lsn;
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, lsn));
        // 读到local_start_lsn会reset shared_storage
        if (lsn == tmp_read_until_log_start_lsn) {
          CLOG_LOG(INFO, "iterate tmp_local_end_lsn");
          break;
        }
      }
      // 设置回收位点
      local_storage.set_start_lsn(local_start_lsn+4*default_block_size);
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, lsn));
        // 读到tmp_read_until_log_start_lsn后会消费shared_stoarge
        if (lsn == tmp_read_until_log_start_lsn + iterator_single_read_size) {
          EXPECT_NE(nullptr, hybrid_storage.shared_storage_.read_buf_.buf_);
        }
        // 读到后会消费shared_stoarge
        if (lsn == tmp_read_until_log_start_lsn + iterator_single_read_size) {
          EXPECT_NE(nullptr, hybrid_storage.shared_storage_.read_buf_.buf_);
          EXPECT_NE(0, hybrid_storage.shared_storage_.read_buf_.buf_len_);
          EXPECT_NE(nullptr, shared_storage.mem_storage_.buf_);
          EXPECT_NE(0, shared_storage.mem_storage_.buf_len_);
        }
      }
      EXPECT_EQ(OB_ITER_END, ret);
      LSN curr_end_lsn = lsn + entry.get_serialize_size();
      CLOG_LOG(INFO, "runlin trace, print curr_end_lsn", K(curr_end_lsn), K(local_end_lsn),
               K(shared_end_lsn));
      EXPECT_EQ(lsn + entry.get_serialize_size(), local_end_lsn);
    }
  }

  // shared | local
  // 5-14   | 10-15
  // 验证mem_storage在迭代过程中被PalfIterator reuse
  {
    CLOG_LOG(INFO, "begin case4");
    int ret = OB_SUCCESS;
    // 将iterator的单次读磁盘大小变小
    const LSN tmp_start_lsn = shared_start_lsn + 2 * default_block_size;
    LogEntry entry;
    LSN lsn;
    {
      S2Iterator s2_iterator_tmp;
      EXPECT_EQ(OB_SUCCESS, s2_iterator_tmp.init(tmp_start_lsn, get_file_end_lsn, &hybrid_storage));
      EXPECT_EQ(OB_SUCCESS, s2_iterator_tmp.next());
      EXPECT_EQ(OB_SUCCESS, s2_iterator_tmp.next());
      EXPECT_EQ(OB_SUCCESS, s2_iterator_tmp.get_entry(entry, lsn));
    }
    shared_storage.reset();
    std::vector<LSN> reuse_lsns = {shared_start_lsn, tmp_start_lsn, lsn};
    for  (auto tmp_reuse_lsn : reuse_lsns) {
      S2Iterator s2_iterator;
      s2_iterator.iterator_impl_.next_round_pread_size_ = iterator_single_read_size;
      EXPECT_EQ(OB_SUCCESS, s2_iterator.init(shared_start_lsn, get_file_end_lsn, &hybrid_storage));
      LSN tmp_lsn;
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, tmp_lsn));
        if (shared_storage.mem_storage_.buf_ != NULL) {
          int64_t tmp_data_len = 0;
          EXPECT_EQ(OB_SUCCESS, shared_storage.mem_storage_.get_data_len(tmp_data_len));
          if (tmp_data_len > 10 * single_log_entry_size && tmp_lsn > shared_storage.mem_storage_.start_lsn_) {
            break;
          }
        }
      }
      s2_iterator.reuse(tmp_reuse_lsn);
      EXPECT_EQ(OB_SUCCESS, s2_iterator.next());
      EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, tmp_lsn));
      EXPECT_EQ(tmp_reuse_lsn, shared_storage.mem_storage_.start_lsn_);
      // reuse后预期mem_storage会被重置
      while (OB_SUCC(s2_iterator.next())) {
        EXPECT_EQ(OB_SUCCESS, s2_iterator.get_entry(entry, tmp_lsn));
      }
      EXPECT_EQ(OB_ITER_END, ret);
      LSN curr_end_lsn = tmp_lsn + entry.get_serialize_size();
      CLOG_LOG(INFO, "runlin trace, print curr_end_lsn", K(curr_end_lsn), K(local_end_lsn),
               K(shared_end_lsn), K(MTL_CPU_COUNT()));
      EXPECT_EQ(tmp_lsn + entry.get_serialize_size(), local_end_lsn);
      EXPECT_EQ(suggessted_read_buf_size, shared_storage.calc_read_size_());
      unittest_tenant_base_->unit_max_cpu_ = 1.0;
      EXPECT_EQ(2*logservice::ObLogSharedStorage::MAX_LOG_SIZE, shared_storage.calc_read_size_());
      unittest_tenant_base_->unit_max_cpu_ = 1000;
    }
  }
  hybrid_storage.local_storage_.palf_handle_guard_.palf_handle_.palf_handle_impl_ = NULL;
}

}
}

int main(int argc, char **argv)
{
  srandom(ObTimeUtility::current_monotonic_time());
  system("rm -rf test_log_iterator_storage.log*");
  OB_LOGGER.set_file_name("test_log_iterator_storage.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_iterator_storage");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
