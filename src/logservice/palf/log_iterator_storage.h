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

#ifndef OCEANBASE_LOGSERVICE_ITERATOR_STORAGE_
#define OCEANBASE_LOGSERVICE_ITERATOR_STORAGE_
#include <cstdint>
#include "lib/file/ob_file.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/function/ob_function.h"   // ObFunction
#include "log_io_context.h"
#include "log_storage_interface.h"
#include "lsn.h"
#include "log_reader_utils.h"
namespace oceanbase
{
namespace palf
{
typedef ObFunction<LSN()> GetFileEndLSN;
class IteratorStorage {
public:
  IteratorStorage();
  virtual ~IteratorStorage() = 0;
  int init(const LSN &start_lsn,
           const int64_t block_size,
           const GetFileEndLSN &get_file_end_lsn,
           ILogStorage *log_storage);
  void destroy();
  void reuse(const LSN &start_lsn);
  inline const LSN get_lsn(const offset_t pos) const
  { return start_lsn_ + pos; }
  inline bool check_iterate_end(const offset_t pos) const
  { return start_lsn_ + pos > get_file_end_lsn_(); }
  int pread(const int64_t pos,
            const int64_t in_read_size,
            char *&buf,
            int64_t &out_read_size,
            LogIOContext &io_ctx);
  VIRTUAL_TO_STRING_KV(K_(start_lsn), K_(end_lsn), K_(read_buf), K_(block_size), KP(log_storage_));
protected:
  inline int64_t get_valid_data_len_()
  { return end_lsn_ - start_lsn_; }
  virtual int read_data_from_storage_(int64_t &pos,
      const int64_t in_read_size,
      char *&buf,
      int64_t &out_read_size,
      LogIOContext &io_ctx) = 0;
protected:
  // update after read_data_from_storage
  // 'start_lsn_' is the base position
  LSN start_lsn_;
  // end_lsn_ is always same sa start_lsn_ + read_buf_.buf_len_, except for reuse
  LSN end_lsn_;
  ReadBuf read_buf_;
  int64_t block_size_;
  ILogStorage *log_storage_;
  GetFileEndLSN get_file_end_lsn_;
  bool is_inited_;
};

class MemoryStorage : public ILogStorage {
public:
  MemoryStorage();
  ~MemoryStorage();
  int init(const LSN &start_lsn);
  void destroy();
  bool is_inited() const { return is_inited_; }
  int append(const char *buf, const int64_t buf_len);
  int pread(const LSN& lsn, const int64_t in_read_size, ReadBuf &read_buf, int64_t &out_read_size, LogIOContext &io_ctx) final;
  TO_STRING_KV(K_(start_lsn), K_(log_tail), K_(buf), K_(buf_len), K_(is_inited));
private:
  const char *buf_;
  int64_t buf_len_;
  LSN start_lsn_;
  LSN log_tail_;
  bool is_inited_;
};

class MemoryIteratorStorage : public IteratorStorage {
public:
  ~MemoryIteratorStorage();
  void destroy();
private:
  int read_data_from_storage_(int64_t &pos,
      const int64_t in_read_size,
      char *&buf,
      int64_t &out_read_size,
      LogIOContext &io_ctx) final;
public:
  INHERIT_TO_STRING_KV("IteratorStorage", IteratorStorage, "IteratorStorageType:", "MemoryIteratorStorage");
};

class DiskIteratorStorage : public IteratorStorage {
public:
  ~DiskIteratorStorage();
  void destroy();
  INHERIT_TO_STRING_KV(
      "IteratorStorage",
      IteratorStorage,
      "IteratorStorageType:",
      "DiskIteratorStorage");

private:
  int read_data_from_storage_(
      int64_t &pos,
      const int64_t in_read_size,
      char *&buf,
      int64_t &out_read_size,
      LogIOContext &io_ctx) final;

  int ensure_memory_layout_correct_(const int64_t pos, const int64_t in_read_size, int64_t &remain_valid_data_size);
  void do_memove_(ReadBuf &dst, const int64_t pos, int64_t &valid_tail_part_size);
};

} // end namespace palf
} // end namespace oceanbase
#endif
