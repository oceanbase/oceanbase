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
#ifndef OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_UTILS_H_
#define OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_UTILS_H_
#include "lib/utility/ob_print_utils.h"                       // TO_STRING_KV
#include "lib/container/ob_fixed_array.h"                     // ObFixedArray
#include "common/storage/ob_device_common.h"                  // ObStorageAccessType
#include "common/storage/ob_io_device.h"                      // ObIODevice
#include "share/io/ob_io_define.h"                            // ObIOHandle
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace logservice
{
int get_and_init_io_device(const common::ObString &uri,
                           const common::ObString &storage_info,
                           const uint64_t storage_id,
                           common::ObIODevice *&io_device);

void release_io_device(common::ObIODevice *&io_device);

enum class OPEN_FLAG{
  INVALID_FLAG = 0,
  READ_FLAG = 1,
  APPEND_FLAG = 2,
  MULTI_UPLOAD_FLAG = 3,
  MAX_FLAG = 4
};

int convert_to_storage_access_type(const OPEN_FLAG &open_flag,
                                   common::ObStorageAccessType &storage_access_type);

int open_io_fd(const common::ObString &uri,
               const OPEN_FLAG open_flag,
               common::ObIODevice *io_device,
               common::ObIOFd &io_fd);

int close_io_fd(common::ObIODevice *io_device,
                const common::ObIOFd &io_fd);

class ObLogExternalStorageCtxItem {
public:
  ObLogExternalStorageCtxItem();
  ObLogExternalStorageCtxItem(const common::ObIOFd &io_fd,
                              common::ObIODevice *io_device);
  ~ObLogExternalStorageCtxItem();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(io_handle), K_(io_fd), KP(io_device_), KP(&io_handle_));
  ObIOHandle io_handle_;
  common::ObIOFd io_fd_;
  common::ObIODevice *io_device_;
};

class ObLogExternalStorageCtx {
public:
  ObLogExternalStorageCtx();
  ~ObLogExternalStorageCtx();
  int init(const ObString &uri,
           const ObString &storage_info,
           const uint64_t storage_id,
           const int64_t concurrency,
           const OPEN_FLAG &flag);
  int wait(int64_t &out_size);
  void destroy();
  int get_item(const int64_t index,
               ObLogExternalStorageCtxItem *&item) const;
  int get_io_fd(ObIOFd &io_fd) const;
  int get_io_device(ObIODevice *&io_device) const;
  // inc count_ after async_pread or async_pwrite success, we need wait these async io request successfully
  int inc_count();
  int64_t get_count() const;
  bool is_valid() const;
  TO_STRING_KV(KP(items_), K_(capacity), K_(count), K_(is_inited));
private:
  ObLogExternalStorageCtxItem* items_;
  int64_t count_;
  int64_t capacity_;
  common::ObIOFd io_fd_;
  common::ObIODevice *io_device_;
  bool is_inited_;
  DISABLE_COPY_ASSIGN(ObLogExternalStorageCtx);
};

class ObLogExternalStorageHandleAdapter {
public:
  ObLogExternalStorageHandleAdapter();
  ~ObLogExternalStorageHandleAdapter();

  // Implement
public:

  int exist(const common::ObString &uri,
            const common::ObString &storage_info,
            bool &exist);

  int get_file_size(const common::ObString &uri,
                    const common::ObString &storage_info,
                    int64_t &file_size);

  int async_pread(const int64_t offset,
                  char *buf,
                  const int64_t read_buf_size,
                  ObLogExternalStorageCtxItem &io_ctx);

  int async_pwrite(const int64_t offset,
                   const char *buf,
                   const int64_t write_buf_size,
                   ObLogExternalStorageCtxItem &io_ctx);
};
} // end namespace logservice
} // end namespace oceanbase
#endif
