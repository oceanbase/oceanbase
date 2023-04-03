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

#ifndef OCEANBASE_UNITEST_LOGSERVICE_MOCK_PALF
#define OCEANBASE_UNITEST_LOGSERVICE_MOCK_PALF
#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "logservice/replayservice/ob_replay_status.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/palf_env.h"

namespace oceanbase
{
namespace palf
{
class MockPalfHandle;
class MockPalfBufferIterator : public PalfBufferIterator
{
public:
  MockPalfBufferIterator() {destroy();}
  virtual ~MockPalfBufferIterator() {destroy();}
  void destroy()
  {
    base_id_ = 0;
    cur_log_size_ = 0;
    base_val_.reset();
    end_ = 2048;
    cb_ = NULL;
  }
  int init(PalfHandle *palf_handle, const LSN &start_lsn)
  {
    UNUSED(palf_handle);
    base_val_ = start_lsn;
    return OB_SUCCESS;
  }
  int reset(const LSN lsn) { UNUSED(lsn); return OB_SUCCESS; }
  bool valid() const { return base_val_.val_ < end_; }
  int next()
  {
    base_val_ = base_val_ + cur_log_size_;
    return OB_SUCCESS;
  }
  int get_entry(char *&buffer, int64_t &nbytes, int64_t &ts, LSN &offset)
  {
    REPLAY_LOG(INFO, "[KEQING] palf get entry", K(end_), K(base_val_), K(base_id_));
    uint64_t flag = base_val_.val_;
    bool is_barrier = (0 == flag % 10);
    int64_t cur_time = oceanbase::common::ObTimeUtility::fast_current_time();
    logservice::ObLogBaseHeader header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE, is_barrier, flag);
    int64_t BUF_SIZE = header.get_serialize_size();
    int64_t pos = 0;
    char *buf = static_cast<char*>(ob_malloc(BUF_SIZE));
    header.serialize(buf, BUF_SIZE, pos);
    buffer = buf;
    nbytes = BUF_SIZE;
    cur_log_size_ = nbytes;
    ts = cur_time;
    offset = base_val_;
    if (flag == 1024) {
      end_ = 4096;
      cb_->update_end_lsn(1, LSN(0, end_));
      REPLAY_LOG(INFO, "[KEQING] update end offset", K(end_));
    }
    return OB_SUCCESS;
  }
  void set_cb(PalfFSCb *fs_cb) { cb_ = fs_cb; }
private:
  static const int64_t TEST_LOG_FILE_SIZE = 5000;
  LSN base_val_;
  int64_t cur_log_size_;
  int64_t base_id_;
  int64_t end_;
  PalfFSCb *cb_;
};

class MockPalfHandle : public PalfHandle
{
public:
  MockPalfHandle() :
    base_val_(0, 0),
    end_val_(0, 1 << 11),
    iterator_()
    {}
  virtual ~MockPalfHandle() {reset();}
  MockPalfHandle(const LSN &base_offset, const LSN &end_offset)
  : iterator_()
  {
    base_val_ = base_offset;
    end_val_ = end_offset;
  }
  void reset()
  {
    base_val_.reset();
    end_val_.reset();
    iterator_.destroy();
  }
  void set(const LSN &base_offset, const LSN &end_offset)
  {
    base_val_ = base_offset;
    end_val_ = end_offset;
  }
  const LSN &get_end_offset() const { return end_val_; }
  const LSN &get_base_offset() const { return base_val_; }
  int64_t get_base_ts() const { return 0; }
  int unregister_file_size_cb() { return OB_SUCCESS; }
  int seek(const LSN &start_lsn,
           PalfBufferIterator &iter)
  {
    iterator_.init(this, start_lsn);
    iter = iterator_;
    return OB_SUCCESS;
  }
  void free_iterator(PalfBufferIterator *iterator)
  {
    ob_free(iterator);
    iterator = NULL;
  }
  int register_file_size_cb(PalfFSCb *fs_cb)
  {
    iterator_.set_cb(fs_cb);
    return OB_SUCCESS;
  }
private:
  LSN base_val_;
  LSN end_val_;
  MockPalfBufferIterator iterator_;
};

class MockPalfEnv : public PalfEnv
{
public:
  MockPalfEnv() :
  palf_handle_()
  {}
  virtual ~MockPalfEnv() {reset();}
  void reset()
  {
    palf_handle_.reset();
  }
  int open(const int64_t id, PalfRoleChangeCb *rc_cb, PalfHandle &handle) final
  {
    UNUSED(id);
    UNUSED(rc_cb);
    handle = palf_handle_;
    REPLAY_LOG(INFO, "KEQING DEBUG open", K(&palf_handle_));
    return OB_SUCCESS;
  }
  int close(PalfHandle *handle) { UNUSED(handle); return OB_SUCCESS; }
private:
  MockPalfHandle &palf_handle_;
};

} // end namesapce logservice
} // end namespace oceanbase

#endif // OCEANBASE_UNITEST_LOGSERVICE_MOCK_PALF
