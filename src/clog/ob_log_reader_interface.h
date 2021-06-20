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

#ifndef OCEANBASE_CLOG_OB_LOG_READER_INTERFACE_
#define OCEANBASE_CLOG_OB_LOG_READER_INTERFACE_

#include "ob_log_entry.h"
#include "ob_log_file_pool.h"

namespace oceanbase {
namespace clog {
// Used to set the parameters of reading files, the purpose is to add
// parameters int the future without changing the interface
struct ObReadParam {
  file_id_t file_id_;
  offset_t offset_;
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
  int64_t read_len_;
  int64_t timeout_;

  ObReadParam();
  ~ObReadParam();
  void reset();
  void shallow_copy(const ObReadParam& new_param);
  TO_STRING_KV(N_FILE_ID, file_id_, N_OFFSET, offset_, N_PARTITION_KEY, partition_key_, N_LOG_ID, log_id_, N_READ_LEN,
      read_len_, N_TIMEOUT, timeout_);

private:
  static const int64_t OB_TIMEOUT = 10000000;  // 10s
};

struct ObReadRes {
  const char* buf_;
  int64_t data_len_;

  ObReadRes();
  ~ObReadRes();
  void reset();
  TO_STRING_KV(KP(buf_), K(data_len_));
};

struct ObReadBuf {
public:
  ObReadBuf() : buf_(NULL), buf_len_(0)
  {}
  ~ObReadBuf()
  {
    reset();
  }
  void reset()
  {
    buf_ = NULL;
    buf_len_ = 0;
  }
  bool is_valid()
  {
    return (NULL != buf_ && buf_len_ > 0);
  }
  bool is_aligned()
  {
    return !(reinterpret_cast<int64_t>(buf_) & (CLOG_DIO_ALIGN_SIZE - 1));
  }
  TO_STRING_KV(KP(buf_), K(buf_len_));

public:
  char* buf_;
  int64_t buf_len_;
};

class ObILogDirectReader {
public:
  enum TSIBufferIdx {
    CACHE_IDX = 0,
    IO_IDX = 1,
    COMPRESS_IDX = 2,
    MAX_IDX,
  };

public:
  ObILogDirectReader()
  {}
  virtual ~ObILogDirectReader()
  {}

  static int alloc_buf(const char* label, ObReadBuf& rbuf);
  static void free_buf(ObReadBuf& rbuf);

  virtual int read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost) = 0;

  virtual int read_data(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq, ObReadBuf& rbuf,
      ObReadRes& res, ObReadCost& cost) = 0;

  virtual int read_log(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq, ObReadBuf& rbuf,
      ObLogEntry& entry, ObReadCost& cost) = 0;

  virtual int read_trailer(
      const ObReadParam& param, ObReadBuf& rbuf, int64_t& start_pos, file_id_t& next_file_id, ObReadCost& cost) = 0;
};

class ObILogInfoBlockReader {
public:
  ObILogInfoBlockReader()
  {}
  virtual ~ObILogInfoBlockReader()
  {}
  virtual int read_info_block_data(const ObReadParam& param, ObReadRes& res) = 0;
  virtual int read_info_block_data(const ObReadParam& param, ObReadRes& res, ObReadCost& cost) = 0;
};

class ObIRawLogIterator {
public:
  ObIRawLogIterator()
  {}
  virtual ~ObIRawLogIterator()
  {}
  virtual void destroy() = 0;
  virtual int next_entry(ObLogEntry& entry, ObReadParam& param, int64_t& persist_len) = 0;
};

class ObIRawIndexIterator {
public:
  ObIRawIndexIterator()
  {}
  virtual ~ObIRawIndexIterator()
  {}
  virtual void destroy() = 0;
  virtual int next_entry(ObIndexEntry& entry, ObReadParam& param, int64_t& persist_len) = 0;
  virtual ObReadCost get_read_cost() const = 0;
};

class ObReadBufGuard {
public:
  ObReadBufGuard(const char* label) : read_buf_()
  {
    ObILogDirectReader::alloc_buf(label, read_buf_);
  }
  virtual ~ObReadBufGuard()
  {
    (void)ObILogDirectReader::free_buf(read_buf_);
  }

  ObReadBuf& get_read_buf()
  {
    return read_buf_;
  }

private:
  ObReadBuf read_buf_;
};

}  // end namespace clog
}  // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_READER_INTERFACE_
