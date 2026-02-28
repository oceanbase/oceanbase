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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_ASYNC_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_ASYNC_H_
#include <opendal.h>
#include "lib/ob_define.h"
#include "obdal/obdal_accessor.h"
#include "ob_i_storage.h"
#include "ob_storage.h"
#include "ob_storage_obdal_base.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace common
{

bool is_valid_async_access_type(const ObStorageAccessType &access_type);
bool is_valid_async_storage_type(const ObStorageType &storage_type);

// the callback function type for users of ObStorageAsync.
// when user use ObStorageAsync, like ObStorageAsyncReader::pread, ObStorageAsyncWriter::write, etc.,
// they need to provide a callback function which type is ObStorageAsyncCallback and a context.
// Then, when obdal async operation is completed, obdal will call the callback function with the result.
// @param ret: the return value of the callback function.
// @param bytes: the number of bytes read or written.
// @param ctx: the context passed to the callback function.
typedef int (*ObStorageAsyncCallback)(int ret, int64_t bytes, void *ctx);

class ObStorageAsyncBase;
// The context for ObStorageAsync. Only use inner.
struct ObStorageAsyncContext
{
public:
  ObStorageAsyncContext(ObStorageAsyncBase *b, ObStorageAsyncCallback c, void *ctx, const int64_t offset, const int64_t expected_bytes)
    : base_(b),
      callback_(c),
      ctx_(ctx),
      offset_(offset),
      expected_bytes_(expected_bytes),
      start_ts_(ObTimeUtility::current_time())
  {}
  ~ObStorageAsyncContext() {}
public:
  ObStorageAsyncBase *base_;
  ObStorageAsyncCallback callback_;
  void *ctx_;
  int64_t offset_;
  int64_t expected_bytes_;
  int64_t start_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncContext);
};

class ObStorageAsyncBase : public ObStorageAccesser, public ObStorageObDalBase
{
public:
  ObStorageAsyncBase();
  virtual ~ObStorageAsyncBase();
  virtual void reset();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual bool is_opened() const { return is_opened_; }
  int alloc_and_assign_async_context(ObStorageAsyncCallback callback,
                                             void *ctx,
                                             ObStorageAsyncContext *&context,
                                             const int64_t offset = 0,
                                             const int64_t expected_bytes = 0);
  void free_async_context(ObStorageAsyncContext *&context);
protected:
  bool is_opened_;
  int64_t file_length_;
  opendal_async_operator *async_op_;
};

class ObStorageAsyncReader : public ObStorageAsyncBase
{
public:
  ObStorageAsyncReader();
  virtual ~ObStorageAsyncReader();
  virtual int open(const common::ObString &uri,
                   common::ObObjectStorageInfo *storage_info,
                   const bool has_meta = true);
  virtual void reset() override;
  int pread(char *buf,
            const int64_t size,
            const int64_t offset,
            ObStorageAsyncCallback cb,
            void *ctx);
  int close();
  bool has_meta() const { return has_meta_; }
  int64_t get_length() const { return file_length_; }
  static void callback(opendal_error *error, int64_t bytes, void *raw_ctx);
protected:
  bool has_meta_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncReader);
};

class ObStorageAsyncWriter : public ObStorageAsyncBase
{
public:
  ObStorageAsyncWriter();
  virtual ~ObStorageAsyncWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual void reset() override;
  int write(const char *buf, const int64_t size, ObStorageAsyncCallback cb, void *ctx);
  int close();
  void set_length(const int64_t length) { ATOMIC_SET(&file_length_, length); }
  int64_t get_length() const { return ATOMIC_LOAD(&file_length_); }
  static void callback(opendal_error *error, int64_t bytes, void *raw_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncWriter);
};

class ObStorageAsyncMultiPartWriter : public ObStorageAsyncBase
{
public:
  ObStorageAsyncMultiPartWriter();
  virtual ~ObStorageAsyncMultiPartWriter();
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual void reset() override;
  virtual int upload_part(const char *buf,
                          const int64_t size,
                          const int64_t part_id,
                          ObStorageAsyncCallback cb,
                          void *ctx);
  virtual int complete();
  virtual int abort();
  virtual int close();
  void add_length(const int64_t length) { ATOMIC_AAF(&file_length_, length); }

  static void callback(opendal_error *error, int64_t bytes, void *raw_ctx);

protected:
  opendal_async_multipart_writer *async_multipart_writer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncMultiPartWriter);
};

class ObStorageAsyncDirectMultiPartWriter : public ObStorageAsyncMultiPartWriter
{
public:
  ObStorageAsyncDirectMultiPartWriter();
  virtual ~ObStorageAsyncDirectMultiPartWriter();
  virtual void reset() override;
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;
  virtual int upload_part(const char *buf,
                          const int64_t size,
                          const int64_t part_id,
                          ObStorageAsyncCallback cb,
                          void *ctx) override;
  virtual int complete() override;
  virtual int abort() override;
  virtual int close() override;

  virtual int buf_append_part(const char *buf,
                              const int64_t size,
                              const uint64_t tenant_id,
                              bool &is_full);
  virtual int get_part_id(bool &is_exist, int64_t &part_id);
  virtual int get_part_size(const int64_t part_id, int64_t &part_size) const;

protected:
  int64_t cur_part_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncDirectMultiPartWriter);
};

class ObStorageAsyncBufferedMultiPartWriter : public ObStorageAsyncDirectMultiPartWriter
{
  using PartData = ObStorageBufferedMultiPartWriter::PartData;
public:
  ObStorageAsyncBufferedMultiPartWriter();
  virtual ~ObStorageAsyncBufferedMultiPartWriter();
  virtual void reset() override;
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) override;

  virtual int upload_part(const char *buf,
                          const int64_t size,
                          const int64_t part_id,
                          ObStorageAsyncCallback cb,
                          void *ctx) override;
  virtual int buf_append_part(const char *buf, const int64_t size, const uint64_t tenant_id, bool &is_full) override;
  virtual int get_part_id(bool &is_exist, int64_t &part_id) override;
  virtual int get_part_size(const int64_t part_id, int64_t &part_size) const override;

protected:
  int append_buf_(const char *buf, const int64_t size, const uint64_t tenant_id);
  int save_buf_to_map_();
  static void free_part_data_(PartData &part_data);

private:
  static constexpr const char *ALLOC_TAG = "BufferdMulti";
  static constexpr int64_t PART_SIZE_THRESHOLD = 6L * 1024L * 1024L; // 6MB

  SpinRWLock lock_;
  char *cur_buf_;
  int64_t cur_buf_pos_;
  hash::ObHashMap<int64_t, PartData> part_id_to_data_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageAsyncBufferedMultiPartWriter);
};


} // namespace common
} // namespace oceanbase
#endif