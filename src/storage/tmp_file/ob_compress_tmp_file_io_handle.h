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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_IO_HANDLE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_IO_HANDLE_H_

#include "storage/tmp_file/ob_tmp_file_io_handle.h"

namespace oceanbase
{
namespace tmp_file
{

class ObCompTmpFileIOHandle final
{
public:
  ObCompTmpFileIOHandle();
  ~ObCompTmpFileIOHandle();
  void reset();
  bool is_valid();
  char *get_buffer();
  TO_STRING_KV(K(io_handle_), K(tenant_id_), K(fd_), K(is_aio_read_),
               KP(decompress_buf_), KP(user_buf_), K(user_buf_size_));

public:
  OB_INLINE void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  OB_INLINE void set_fd(int64_t fd)
  {
    fd_ = fd;
  }
  OB_INLINE void set_is_aio_read(bool is_aio_read)
  {
    is_aio_read_ = is_aio_read;
  }
  OB_INLINE void set_decompress_buf(char* decompress_buf)
  {
    decompress_buf_ = decompress_buf;
  }
  OB_INLINE void set_user_buf(char* user_buf)
  {
    user_buf_ = user_buf;
  }
  OB_INLINE void set_user_buf_size(const int64_t user_buf_size)
  {
    user_buf_size_ = user_buf_size;
  }
  OB_INLINE ObTmpFileIOHandle& get_basic_io_handle()
  {
    return io_handle_;
  }
  OB_INLINE uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  OB_INLINE int64_t get_fd()
  {
    return fd_;
  }
  OB_INLINE char* get_decompress_buf()
  {
    return decompress_buf_;
  }
  OB_INLINE char* get_user_buf()
  {
    return user_buf_;
  }
  OB_INLINE int64_t get_user_buf_size()
  {
    return user_buf_size_;
  }

private:
  ObTmpFileIOHandle io_handle_;
  uint64_t tenant_id_;
  int64_t fd_;
  bool is_aio_read_;
  char *decompress_buf_;
  char *user_buf_;
  int64_t user_buf_size_;
  DISALLOW_COPY_AND_ASSIGN(ObCompTmpFileIOHandle);
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_IO_HANDLE_H_
