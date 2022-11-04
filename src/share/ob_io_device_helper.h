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

#ifndef OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_
#define OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_

#include <stdint.h>
#include "common/storage/ob_io_device.h"
#include "share/ob_local_device.h"

namespace oceanbase
{
namespace share
{
class ObGetFileIdRangeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObGetFileIdRangeFunctor(const char *dir)
    : dir_(dir),
      min_file_id_(OB_INVALID_FILE_ID),
      max_file_id_(OB_INVALID_FILE_ID)
  {
  }
  virtual ~ObGetFileIdRangeFunctor() = default;

  virtual int func(const dirent *entry) override;
  uint32_t get_min_file_id() const { return min_file_id_; }
  uint32_t get_max_file_id() const { return max_file_id_; }
private:
  const char *dir_;
  uint32_t min_file_id_;
  uint32_t max_file_id_;

  DISALLOW_COPY_AND_ASSIGN(ObGetFileIdRangeFunctor);
};

class ObGetFileSizeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObGetFileSizeFunctor(const char* dir)
    : dir_(dir), total_size_(0)
  {
  }
  virtual ~ObGetFileSizeFunctor() = default;

  virtual int func(const dirent *entry) override;
  int64_t get_total_size() const { return total_size_; }
private:
  const char* dir_;
  int64_t total_size_;

  DISALLOW_COPY_AND_ASSIGN(ObGetFileSizeFunctor);
};

/*this class seems like the adapter for local/ofs device*/
class ObIODeviceWrapper final
{
public:
  static ObIODeviceWrapper &get_instance();

  int init(
      const char *data_dir,
      const char *sstable_dir,
      const int64_t block_size,
      const int64_t data_disk_percentage,
      const int64_t data_disk_size);
  void destroy();

  ObIODevice& get_local_device() {abort_unless(NULL != local_device_); return *local_device_; }

private:
  ObIODeviceWrapper();
  ~ObIODeviceWrapper();

private:
  ObLocalDevice *local_device_;
  bool is_inited_;
};

#define LOCAL_DEVICE_INSTANCE ::oceanbase::share::ObIODeviceWrapper::get_instance().get_local_device()
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_
