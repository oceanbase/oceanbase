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
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif
namespace oceanbase
{
namespace palf
{
typedef ObFunction<LSN()> GetFileEndLSN;
class IteratorStorage
{
public:
  IteratorStorage();
  ~IteratorStorage();
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
  TO_STRING_KV(K_(start_lsn), K_(end_lsn), K_(read_buf), K_(block_size), KP(log_storage_), KPC(log_storage_),
               "storage_type", (NULL == log_storage_ ? "dummy" : log_storage_->get_log_storage_type_str()));
private:
  int read_data_from_storage_(
      int64_t &pos,
      const int64_t in_read_size,
      char *&buf,
      int64_t &out_read_size,
      LogIOContext &io_ctx);

  int ensure_memory_layout_correct_(const int64_t pos,
                                    const int64_t in_read_size,
                                    int64_t &remain_valid_data_size);
  void do_memove_(ReadBuf &dst,
                  const int64_t pos,
                  int64_t &valid_tail_part_size);
  bool is_memory_storage_() const
  { return ILogStorageType::MEMORY_STORAGE == log_storage_->get_log_storage_type(); }
  bool is_hybrid_storage_() const
  { return ILogStorageType::HYBRID_STORAGE == log_storage_->get_log_storage_type(); }
  inline int64_t get_valid_data_len_()
  { return end_lsn_ - start_lsn_; }
private:
  // update after read_data_from_storage
  // 'start_lsn_' is the base position
  LSN start_lsn_;
  // end_lsn_ is always same sa start_lsn_ + read_buf_.buf_len_, except for reuse
  LSN end_lsn_;
  ReadBuf read_buf_;
  int64_t block_size_;
  ILogStorage *log_storage_;
  GetFileEndLSN get_file_end_lsn_;
  LogIOContext io_ctx_;
  bool is_inited_;
};

class MemoryStorage : public ILogStorage {
public:
  MemoryStorage();
  ~MemoryStorage();
  int init(const LSN &start_lsn, const bool enable_logservice);
  void reset();
  void destroy();
  bool is_inited() const { return is_inited_; }
  int append(const char *buf, const int64_t buf_len);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const libpalf::LibPalfIteratorMemoryStorageFFI * get_memory_storage() { return memory_storage_; }
#endif
  int pread(const LSN& lsn,
	    const int64_t in_read_size,
	    ReadBuf &read_buf,
	    int64_t &out_read_size,
            LogIOContext &io_ctx) final;
  INHERIT_TO_STRING_KV("ILogStorage", ILogStorage, K_(start_lsn), K_(log_tail), KP(buf_), K_(buf_len), K_(is_inited));
private:
  const char *buf_;
  int64_t buf_len_;
  LSN start_lsn_;
  LSN log_tail_;
  bool is_inited_;
  bool enable_logservice_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const libpalf::LibPalfIteratorMemoryStorageFFI *memory_storage_;
#endif
};

} // end namespace palf
} // end namespace oceanbase
#endif
