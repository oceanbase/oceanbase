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

#include <fcntl.h>
#include <gtest/gtest.h>
#include "common/ob_member.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/palf/log_define.h"
#define private public
#include "logservice/palf/log_engine.h"
#undef private
#include "logservice/palf/log_group_entry.h"
#include "share/scn.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_io_worker.h"
#include "logservice/palf/log_iterator_storage.h"
#include "logservice/palf/log_meta.h"
#include "logservice/palf/log_meta_info.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/palf_iterator.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/ob_errno.h"
namespace oceanbase
{
using namespace common;
using namespace palf;
namespace unittest
{

class DummyBlockPool : public palf::ILogBlockPool {
public:
  virtual int create_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path,
                              const int64_t block_size)
  {
    int fd = -1;
    if (-1 == (fd = ::openat(dir_fd, block_path, palf::LOG_WRITE_FLAG | O_CREAT, palf::FILE_OPEN_MODE))) {
      return OB_IO_ERROR;
    } else if (-1 == ::fallocate(fd, 0, 0, PALF_PHY_BLOCK_SIZE)) {
      return OB_IO_ERROR;
    }
    PALF_LOG(INFO, "create_block_at success", K(dir_fd), K(block_path), K(block_size), K(fd));
    ::close(fd);
    return OB_SUCCESS;
  }
  virtual int remove_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path)
  {
    if (-1 == ::unlinkat(dir_fd, block_path, 0)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
};

class PalfPerformanceUnittest
{
public:
  PalfPerformanceUnittest() : allocator_(0), start_lsn_(PALF_INITIAL_LSN_VAL)
  {}
  ~PalfPerformanceUnittest()
  {}
  int init(const int64_t palf_id, const char *log_dir, const LSN &base_lsn, const int64_t base_lsn_ts)
  {
    int ret = OB_SUCCESS;
    log_meta_.generate_by_default(AccessMode::APPEND, LogReplicaType::NORMAL_REPLICA);
    log_meta_.log_snapshot_meta_.base_lsn_ = LSN(5 * PALF_BLOCK_SIZE);
    const int64_t new_palf_epoch = ATOMIC_AAF(&palf_epoch_, 1);
    if (OB_FAIL(log_engine_.init(palf_id, log_dir, log_meta_, &allocator_, &log_block_pool_, \
            &log_rpc_, &log_io_worker_wrapper_, new_palf_epoch))) {
      PALF_LOG(WARN, "LogEngine init failed", K(ret));
    }
    start_lsn_ = base_lsn;
    curr_lsn_ = base_lsn;
    start_lsn_ts_ = base_lsn_ts;
    return ret;
  }
  int load(const int64_t palf_id, const char *log_dir)
  {
    int ret = OB_SUCCESS;
    // log_engine_.destroy();
    LogGroupEntryHeader entry_header;
    const int64_t new_palf_epoch = ATOMIC_AAF(&palf_epoch_, 1);
    ret = log_engine_.load(palf_id, log_dir, &allocator_, &log_block_pool_, &log_rpc_, &log_io_worker_wrapper_, \
        entry_header, new_palf_epoch);
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
    log_engine_.get_block_id_range(min_block_id, max_block_id);
    start_lsn_ = log_engine_.log_storage_.log_tail_;
    curr_lsn_ = start_lsn_;
    start_lsn_ts_ = ObTimeUtility::current_time_ns();
    PALF_LOG(INFO, "runlin trace get_block_id_range", K(ret), K(min_block_id), K(max_block_id));
    return ret;
  }
  int generate_data(
      LogWriteBuf &write_buf, char *buf, int buf_len, const LSN &lsn, const int64_t pid, const int64_t log_ts)
  {
    int ret = OB_SUCCESS;
    int64_t sum;
    int64_t pos = 0;
    LogGroupEntryHeader header;
    memset(buf, 'c', buf_len);
    LogEntryHeader e_header;
    LogEntry e_entry;
    LogGroupEntry entry;
    const int64_t log_entry_data_size = 500;
    int64_t remain_size = buf_len - sizeof(header);
    while (remain_size > 0) {
      int64_t curr_log_entry_sz = MIN(log_entry_data_size, remain_size);
      if (remain_size - curr_log_entry_sz <= sizeof(header)) {
        curr_log_entry_sz += (remain_size - curr_log_entry_sz);
      }
      OB_ASSERT(
          OB_SUCCESS == e_header.generate_header(
                            buf + sizeof(header) + sizeof(e_header) + pos, curr_log_entry_sz - sizeof(e_header), 1));
      OB_ASSERT(OB_SUCCESS == e_header.serialize(buf + sizeof(header), buf_len, pos));
      remain_size -= curr_log_entry_sz;
      pos += (curr_log_entry_sz - sizeof(e_header));
    }
    pos = 0;
    write_buf.push_back(buf, buf_len);
    OB_ASSERT(OB_SUCCESS == header.generate(false, false, write_buf, buf_len-sizeof(header), 1, 1, lsn, pid, sum));
    header.update_header_checksum();
    OB_ASSERT(OB_SUCCESS == header.serialize(buf, buf_len, pos));
    return ret;
  }

  int ping_buf(const block_id_t min_block_id, const block_id_t max_block_id, char *&buf);

  int test_log_engine_truncaete(const LSN &lsn)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(log_engine_.truncate(lsn))) {
    } else {
      start_lsn_ = lsn;
      PALF_LOG(INFO, "test_log_engine_truncaete success", K(ret), K(lsn), K(start_lsn_));
    }
    return ret;
  }
  int test_log_engine_append_log_meta()
  {
    int ret = 0;
    int count = 3234;
    const int64_t init_proposal_id = 1;
    LogConfigMeta meta;
    LogConfigInfo info;
    LogConfigVersion version;
    ObAddr server;
    server.set_ip_addr("127.0.0.1", 12345);
    ObMember member(server, 1);
    ObMemberList default_mlist;
    default_mlist.add_member(member);
    GlobalLearnerList learners;
    version.generate(1, 1);
    learners.add_learner(member);
    info.config_version_ = version;
    EXPECT_EQ(OB_SUCCESS, info.generate(default_mlist, 1, learners, version));
    meta.generate(init_proposal_id, info, info);
    log_engine_.log_meta_.update_log_config_meta(meta);
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, log_engine_.append_log_meta_(log_engine_.log_meta_));
      count--;
    }
    return ret;
  }

  bool need_generaete_padding(const LSN &log_tail, const int64_t curr_write_size)
  {
    bool need_padding = false;
    const int64_t val = PALF_BLOCK_SIZE - (lsn_2_offset(log_tail, PALF_BLOCK_SIZE) + curr_write_size);
    if (0 > val || val < 4 * 1024) {
      need_padding = true;
    }
    return need_padding;
  }
  int test_log_read_block_scn(const block_id_t block_id)
  {
    share::SCN min_scn;
    return log_engine_.get_block_min_scn(block_id, min_scn);
  }
  int test_log_engine_append_log(const std::function<int64_t()> &write_size, const int64_t total_size)
  {
    char *buf = static_cast<char *>(ob_malloc_align(128 * 1024 * 1024, MAX_LOG_BUFFER_SIZE, "dummy"));
    memset(buf, 0, MAX_LOG_BUFFER_SIZE);
    int ret = OB_SUCCESS;
    LSN lsn = curr_lsn_;
    ;
    int64_t remain_size = total_size;
    int64_t log_ts = start_lsn_ts_;
    int64_t pid(1);

    while (remain_size > 0 && OB_SUCC(ret)) {
      LogWriteBuf write_buf;
      int64_t curr_write_size = write_size();
      if (true == need_generaete_padding(lsn, curr_write_size)) {
        const int64_t buf_size = PALF_BLOCK_SIZE - lsn_2_offset(lsn, PALF_BLOCK_SIZE);
        generate_data(write_buf, buf, buf_size, lsn, pid, log_ts);
        if (OB_FAIL(log_engine_.append_log(lsn, write_buf, log_ts))) {
          PALF_LOG(WARN, "append_log faild", K(ret), K(lsn), K(write_buf));
        } else {
          lsn = lsn + write_buf.get_total_size();
          log_ts++;
          PALF_LOG(TRACE, "generaet padding success", K(lsn), K(buf_size), K(curr_write_size));
        }
      } else {
        generate_data(write_buf, buf, curr_write_size, lsn, pid, log_ts);
      }
      if (OB_SUCCESS != ret) {
        PALF_LOG(WARN, "generate padding failed", K(ret), K(lsn), K(curr_write_size));
      } else if (OB_FAIL(log_engine_.append_log(lsn, write_buf, log_ts))) {
        PALF_LOG(WARN, "append_log faild", K(ret), K(lsn), K(write_buf));
      } else {
        remain_size -= curr_write_size;
        lsn = lsn + write_buf.get_total_size();
        log_ts++;
        PALF_LOG(TRACE, "runlin trace", K(curr_write_size), K(lsn));
      }
    }
    curr_lsn_ = lsn;
    return ret;
  }
  LogEngine *get_log_engine()
  {
    return &log_engine_;
  }
  LSN get_start_lsn()
  {
    return start_lsn_;
  }
  int64_t get_start_lsn_ts()
  {
    return start_lsn_ts_;
  }

private:
  const char *log_dir_ = "test_1";
  ObTenantMutilAllocator allocator_;
  LogMeta log_meta_;
  LogEngine log_engine_;
  LogRpc log_rpc_;
  LogIOWorker log_io_worker_wrapper_;
  LSN start_lsn_;
  LSN curr_lsn_;
  DummyBlockPool log_block_pool_;
  int64_t start_lsn_ts_;
  int64_t palf_epoch_;
};

class TestPalfPerformance : public ::testing::Test {
public:
  TestPalfPerformance()
  {}
  virtual ~TestPalfPerformance()
  {}
  static void SetUpTestCase()
  {
    const char *base_dir = "test_1";
    char path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    snprintf(path, OB_MAX_FILE_NAME_LENGTH, "%s/%ld", base_dir, palf_id_);
    char shell[OB_MAX_FILE_NAME_LENGTH] = "mkdir -p ";
    snprintf(shell + strlen(shell), OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, "log");
    system(shell);
    char shell1[OB_MAX_FILE_NAME_LENGTH] = "mkdir -p ";
    snprintf(shell1 + strlen(shell1), OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, "meta");
    std::cout << shell1 << std::endl;
    system(shell1);
    LSN base_lsn(0 * PALF_BLOCK_SIZE);
    int64_t base_lsn_ts(ObTimeUtility::current_time_ns());
    OB_ASSERT(OB_SUCCESS == unittest_.init(palf_id_, path, base_lsn, base_lsn_ts));
    //OB_ASSERT(OB_SUCCESS == unittest_.load(palf_id_, path));
  }
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
  static PalfPerformanceUnittest unittest_;
  static const int64_t palf_id_ = 1;
};

PalfPerformanceUnittest TestPalfPerformance::unittest_;

 TEST_F(TestPalfPerformance, append_log_meta)
{
  int ret = OB_SUCCESS;
  LogEngine *log_engine = unittest_.get_log_engine();
  EXPECT_EQ(OB_SUCCESS, unittest_.test_log_engine_append_log_meta());
  PalfMetaBufferIterator iterator;
  const LSN start_lsn(lsn_2_block(log_engine->get_log_meta_storage()->log_tail_, PALF_META_BLOCK_SIZE));
  auto func = []() {return LSN(LOG_INVALID_LSN_VAL);};
  EXPECT_EQ(OB_SUCCESS, iterator.init(start_lsn, log_engine->get_log_meta_storage(), func));
  LogMetaEntry entry;
  LSN lsn;
  while (OB_SUCC(iterator.next())) {
    EXPECT_EQ(OB_SUCCESS, iterator.get_entry(entry, lsn));
  }
  EXPECT_EQ(OB_ITER_END, ret);
}


int PalfPerformanceUnittest::ping_buf(const block_id_t min_block_id, const block_id_t max_block_id, char *&buf)
{
  int ret = OB_SUCCESS;
  int64_t size = (max_block_id - min_block_id) * PALF_BLOCK_SIZE;
  buf = nullptr;
  PALF_LOG(INFO, "runlin trace ping buf before alloc");
  buf = reinterpret_cast<char *>(ob_malloc_align(4 * 1024, size, "Dummy"));
  PALF_LOG(INFO, "runlin trace ping buf after alloc");
  OB_ASSERT(nullptr != buf);
  memset(buf, 0, size);
  PALF_LOG(INFO, "runlin trace ping buf after memset");
  int64_t cursor = 0;
  for (block_id_t block_id = min_block_id; block_id < max_block_id && OB_SUCC(ret); block_id++) {
    ReadBuf read_buf(buf + cursor, PALF_BLOCK_SIZE);
    int64_t out_read_size = 0;
    if (OB_FAIL(
            get_log_engine()->read_log(LSN(block_id * PALF_BLOCK_SIZE), PALF_BLOCK_SIZE, read_buf, out_read_size))) {
    } else {
      cursor += PALF_BLOCK_SIZE;
      PALF_LOG(INFO, "runlin trace ping buf read block_id", K(block_id));
    }
  }
  return ret;
}

TEST_F(TestPalfPerformance, append_log_random_size)
{
  const int64_t KB = 1024;
  const int64_t US = 1000 * 1000;
  const int64_t total_size = 2* 10 * 64 * KB * KB;
  std::vector<int> size_vec = {2*1024*1024};
  // std::vector<int> size_vec = {1024 * KB};
  for (auto size : size_vec) {
    std::function<int64_t()> write_size = [&]() { return size; };
    std::cout << "test " << size / KB / KB << "(MB)" << std::endl;
    const int64_t start_ts = ObTimeUtility::current_time();
    EXPECT_EQ(OB_SUCCESS, unittest_.test_log_engine_append_log(write_size, total_size));
    const double cost_ts = static_cast<double>(ObTimeUtility::current_time() - start_ts) / US;
    const double bw = (static_cast<double>(total_size) / cost_ts) / (KB * KB);
    std::cout << "writev BW(MB/s): " << bw << "\tcost_ts(s):" << cost_ts << "\ttotal_size(MB):" << total_size / KB / KB
              << std::endl;
    LogEngine *log_engine = unittest_.get_log_engine();
    PalfGroupBufferIterator iterator1;

    const int64_t start_ts1 = ObTimeUtility::current_time();
    const LSN start_lsn1(unittest_.get_start_lsn());
    auto func = []() { return LSN(LOG_MAX_LSN_VAL); };
    EXPECT_EQ(OB_SUCCESS, iterator1.init(start_lsn1, log_engine->get_log_storage(), func));
    int ret = OB_SUCCESS;
    LogGroupEntry entry1;
    LSN lsn1;
    int64_t nbytes, ts;
    while (OB_SUCC(iterator1.next())) {
      // EXPECT_EQ(OB_SUCCESS, iterator1.get_entry(entry1, lsn1));
      EXPECT_EQ(OB_SUCCESS, iterator1.get_entry(entry1, lsn1));
    }
    EXPECT_EQ(OB_ITER_END, ret);
    const double cost_ts1 = static_cast<double>(ObTimeUtility::current_time() - start_ts1) / US;
    const int64_t total_size1 = lsn1 - start_lsn1;
    const double bw1 = (static_cast<double>(total_size1) / cost_ts1) / (KB * KB);
    std::cout << "read group entry BW(MB/s): " << bw1 << "\tcost_ts(s):" << cost_ts1
              << "\ttotal_size(MB):" << total_size1 / KB / KB << std::endl;

    const int64_t start_ts2 = ObTimeUtility::current_time();
    iterator1.destroy();
    PalfBufferIterator iterator2;
    const LSN start_lsn2(unittest_.get_start_lsn());
    EXPECT_EQ(OB_SUCCESS, iterator2.init(start_lsn2, log_engine->get_log_storage(), func));
    LogEntry entry2;
    LSN lsn2;
    while (OB_SUCC(iterator2.next())) {
      // EXPECT_EQ(OB_SUCCESS, iterator2.get_entry(entry2, lsn2));
      EXPECT_EQ(OB_SUCCESS, iterator2.get_entry(entry2.buf_, nbytes, ts, lsn2));
    }
    //EXPECT_EQ(OB_ITER_END, ret);
    const double cost_ts2 = static_cast<double>(ObTimeUtility::current_time() - start_ts2) / US;
    const int64_t total_size2 = lsn2 - start_lsn2;
    const double bw2 = (static_cast<double>(total_size2) / cost_ts2) / (KB * KB);
    std::cout << "read log entry BW(MB/s): " << bw2 << "\tcost_ts(s):" << cost_ts2
              << "\ttotal_size(MB):" << total_size2 / KB / KB << std::endl;
    iterator2.destroy();

    MemPalfBufferIterator iterator3;
    MemoryStorage mem_storage;
    char *buf = nullptr;
    block_id_t min_block_id, max_block_id;
    if (OB_FAIL(unittest_.get_log_engine()->get_block_id_range(min_block_id, max_block_id))) {
      PALF_LOG(WARN, "get_block_id_range failed", K(ret));
    } else {
      max_block_id = min_block_id + 50;
      if (OB_FAIL(unittest_.ping_buf(min_block_id, max_block_id, buf))) {
        PALF_LOG(ERROR, "ping_buf failed", K(ret));
      } else if (OB_FAIL(mem_storage.init(LSN(min_block_id * PALF_BLOCK_SIZE)))) {
        PALF_LOG(ERROR, "mem_storage init failed", K(ret));
      } else if (OB_FAIL(mem_storage.append(buf, PALF_BLOCK_SIZE * (max_block_id - min_block_id)))) {
        PALF_LOG(ERROR, "append failed", K(ret));
      } else if (OB_FAIL(iterator3.init(LSN(min_block_id * PALF_BLOCK_SIZE), &mem_storage, func))) {
        PALF_LOG(ERROR, "init failed", K(ret));
      } else {
        const int64_t start_ts3 = ObTimeUtility::current_time();
        LogEntry entry3;
        LSN lsn3;
        while (OB_SUCC(iterator3.next())) {
          // EXPECT_EQ(OB_SUCCESS, iterator3.get_entry(entry3, lsn3));
          EXPECT_EQ(OB_SUCCESS, iterator2.get_entry(entry3.buf_, nbytes, ts, lsn3));
        }
        //EXPECT_EQ(OB_ITER_END, ret);
        const double cost_ts3 = static_cast<double>(ObTimeUtility::current_time() - start_ts3) / US;
        const int64_t total_size3 = PALF_BLOCK_SIZE * (max_block_id - min_block_id);
        const double bw3 = (static_cast<double>(total_size3) / cost_ts3) / (KB * KB);
        std::cout << "MemoryStorage read log entry BW(MB/s): " << bw3 << "\tcost_ts(s):" << cost_ts3
                  << "\ttotal_size(MB):" << total_size3 / KB / KB << std::endl;
      }
    }
    iterator3.destroy();
    EXPECT_EQ(OB_SUCCESS, iterator3.init(LSN(min_block_id * PALF_BLOCK_SIZE), &mem_storage, func));
  }
  LogMeta log_meta = unittest_.get_log_engine()->get_log_meta();
  LogInfo prev_log_info;
  prev_log_info.log_id_ = 100000000;
  prev_log_info.log_ts_ = 100000000000;
  prev_log_info.lsn_ = LSN(10000*PALF_BLOCK_SIZE);
  prev_log_info.log_proposal_id_ = 1000;
  prev_log_info.accum_checksum_ = 10000000000000000;
  LSN base_lsn(10019 * PALF_BLOCK_SIZE);
  LogSnapshotMeta snapshot_meta; EXPECT_EQ(OB_SUCCESS, snapshot_meta.generate(base_lsn, prev_log_info));
  EXPECT_EQ(OB_SUCCESS, log_meta.update_log_snapshot_meta(snapshot_meta));
  EXPECT_EQ(OB_SUCCESS, unittest_.get_log_engine()->append_log_meta_(log_meta));
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./palf_performance_unittest.log*");
  system("rm -rf ./test_1");
  system("mkdir ./test_1");
  OB_LOGGER.set_file_name("palf_performance_unittest.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::palf_performance_unittest");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
