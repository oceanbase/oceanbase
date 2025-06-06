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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_DEFINE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_DEFINE_H_

#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOInfo;

class ObTmpFileIOHandle final
{
public:
  ObTmpFileIOHandle();
  ~ObTmpFileIOHandle();
  int init_read(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info);
  int init_pread(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, const int64_t read_offset);
  int wait();
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(fd_), K(ctx_),
               KP(buf_), K(update_offset_in_file_),
               K(buf_size_), K(done_size_),
               K(read_offset_in_file_));
public:
  OB_INLINE char *get_buffer() { return buf_; }
  OB_INLINE int64_t get_done_size() const { return done_size_; }
  OB_INLINE int64_t get_buf_size() const { return buf_size_; }
  OB_INLINE ObTmpFileIOReadCtx &get_io_ctx() { return ctx_; }
  OB_INLINE bool is_finished() const { return done_size_ == buf_size_; }
public:
  OB_INLINE uint64_t get_tenant_id() { return tenant_id_; }
  OB_INLINE bool get_is_disable_page_cache() { return ctx_.is_disable_page_cache(); }
  OB_INLINE ObIOFlag get_io_flag() { return ctx_.get_io_flag(); }
  OB_INLINE int64_t get_io_timeout_ms() { return ctx_.get_io_timeout_ms(); }
private:
  int handle_finished_ctx_(ObTmpFileIOReadCtx &ctx);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t fd_;
  ObTmpFileIOReadCtx ctx_;
  char *buf_;
  bool update_offset_in_file_;
  int64_t buf_size_;    // excepted total read size
  int64_t done_size_;   // has finished read size
  int64_t read_offset_in_file_; // records the beginning read offset for current read ctx

  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOHandle);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_DEFINE_H_
