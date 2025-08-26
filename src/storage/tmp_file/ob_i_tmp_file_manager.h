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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_MANAGER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "lib/container/ob_array.h"
#include "storage/tmp_file/ob_i_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"

namespace oceanbase
{
namespace tmp_file
{
class ObITenantTmpFileManager
{
public:
  typedef common::ObLinearHashMap<ObTmpFileKey, ObITmpFileHandle> TmpFileMap;
public:
  ObITenantTmpFileManager();
  virtual ~ObITenantTmpFileManager();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  OB_INLINE bool is_running() const { return is_running_; }

public:
  virtual int alloc_dir(int64_t &dir_id) = 0;
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label) = 0;
  int remove(const int64_t fd);

  int aio_read(const uint64_t tenant_id,
               const ObTmpFileIOInfo &io_info,
               ObTmpFileIOHandle &io_handle,
               ObITmpFileHandle* file_handle_hint = nullptr);

  int aio_pread(const uint64_t tenant_id,
                const ObTmpFileIOInfo &io_info,
                const int64_t offset,
                ObTmpFileIOHandle &io_handle,
                ObITmpFileHandle* file_handle_hint = nullptr);

  int read(const uint64_t tenant_id,
           const ObTmpFileIOInfo &io_info,
           ObTmpFileIOHandle &io_handle,
           ObITmpFileHandle* file_handle_hint = nullptr);

  int pread(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            const int64_t offset,
            ObTmpFileIOHandle &io_handle,
            ObITmpFileHandle* file_handle_hint = nullptr);

  // NOTE:
  //   only support append write.
  int write(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            ObITmpFileHandle* file_handle_hint = nullptr);

  int truncate(const int64_t fd, const int64_t offset, ObITmpFileHandle* file_handle_hint = nullptr);

public:
  virtual int get_tmp_file(const int64_t fd, ObITmpFileHandle &file_handle);

  int get_tmp_file_size(const int64_t fd,
                        int64_t &size,
                        ObITmpFileHandle* file_handle_hint = nullptr);

  virtual int get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size) = 0;

public:
  //for virtual table to show
  int get_tmp_file_fds(ObIArray<int64_t> &fd_arr);
  int get_tmp_file_info(const int64_t fd, ObTmpFileBaseInfo &tmp_file_info);

public:
  void set_compressible_info(const int64_t fd,
                             const OB_TMP_FILE_TYPE file_type,
                             const int64_t compressible_fd,
                             const void* compressible_file);

public:
  virtual int get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                const int64_t write_cache_size_expected_reside_in_memory) = 0;

protected:
  int get_tmp_file_(const int64_t fd,
                    ObITmpFileHandle* file_handle_hint,
                    ObITmpFileHandle*& file_handle_ptr,
                    ObITmpFileHandle &file_handle);

protected:
  virtual int init_sub_module_() = 0;
  virtual int start_sub_module_() = 0;
  virtual int stop_sub_module_() = 0;
  virtual int wait_sub_module_() = 0;
  virtual int destroy_sub_module_() = 0;

protected:
  class CollectTmpFileKeyFunctor final
  {
  public:
    CollectTmpFileKeyFunctor(ObIArray<int64_t> &fds)
        : fds_(fds) {}
    bool operator()(const ObTmpFileKey &key, const ObITmpFileHandle &tmp_file_handle);

  private:
    ObIArray<int64_t> &fds_;
  };

protected:
  bool is_inited_;
  bool is_running_;
  uint64_t tenant_id_;
  common::ObConcurrentFIFOAllocator tmp_file_allocator_;
  common::ObFIFOAllocator callback_allocator_;
  TmpFileMap files_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_MANAGER_H_
