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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/tmp_file/ob_ss_tmp_file_manager.h"
#endif
#include "storage/tmp_file/ob_sn_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTenantTmpFileManager
{
public:
  ObTenantTmpFileManager(): is_inited_(false) {}
  virtual ~ObTenantTmpFileManager() { destroy(); }
  static int mtl_init(ObTenantTmpFileManager *&manager);
  virtual ObSNTenantTmpFileManager &get_sn_file_manager() { return sn_file_manager_; }
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSTenantTmpFileManager &get_ss_file_manager() { return ss_file_manager_; }
#endif
  virtual int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int alloc_dir(int64_t &dir_id);
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label);
  int remove(const int64_t fd);

public:
  int aio_read(const uint64_t tenant_id,
               const ObTmpFileIOInfo &io_info,
               ObTmpFileIOHandle &io_handle,
               ObITmpFileHandle* file_handle = nullptr);

  int aio_pread(const uint64_t tenant_id,
                const ObTmpFileIOInfo &io_info,
                const int64_t offset,
                ObTmpFileIOHandle &io_handle,
                ObITmpFileHandle* file_handle = nullptr);

  int read(const uint64_t tenant_id,
           const ObTmpFileIOInfo &io_info,
           ObTmpFileIOHandle &io_handle,
           ObITmpFileHandle* file_handle = nullptr);

  int pread(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            const int64_t offset,
            ObTmpFileIOHandle &io_handle,
            ObITmpFileHandle* file_handle = nullptr);

  // NOTE:
  //   only support append write.
  int write(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            ObITmpFileHandle* file_handle = nullptr);

  int truncate(const uint64_t tenant_id,
               const int64_t fd,
               const int64_t offset,
               ObITmpFileHandle* file_handle = nullptr);

  int seal(const uint64_t tenant_id,
           const int64_t fd,
           ObITmpFileHandle* file_handle = nullptr);

  int get_tmp_file_size(const uint64_t tenant_id,
                        const int64_t fd,
                        int64_t &file_size,
                        ObITmpFileHandle* file_handle = nullptr);

  int get_tmp_file(const int64_t fd, ObITmpFileHandle &handle);
  int get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size);
  int get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                                     const int64_t write_cache_size_expected_reside_in_memory = 0);
public:
  //for virtual table to show
  int get_tmp_file_fds(ObIArray<int64_t> &fd_arr);
  int get_tmp_file_info(const int64_t fd, ObTmpFileBaseInfo *tmp_file_info);
  void set_compressible_info(const int64_t fd,
                             const OB_TMP_FILE_TYPE file_type,
                             const int64_t compressible_fd,
                             const void* compressible_file);
private:
  bool is_inited_;
  ObSNTenantTmpFileManager sn_file_manager_;

#ifdef OB_BUILD_SHARED_STORAGE
  ObSSTenantTmpFileManager ss_file_manager_;
#endif
};

class ObTenantTmpFileManagerWithMTLSwitch final
{
public:
  static ObTenantTmpFileManagerWithMTLSwitch &get_instance();
  int alloc_dir(const uint64_t tenant_id, int64_t &dir_id);
  int open(const uint64_t tenant_id,
           int64_t &fd,
           const int64_t &dir_id,
           const char* const label = nullptr);
  int remove(const uint64_t tenant_id, const int64_t fd);

public:
  int aio_read(const uint64_t tenant_id,
               const ObTmpFileIOInfo &io_info,
               ObTmpFileIOHandle &io_handle,
               ObITmpFileHandle* file_handle = nullptr);

  int aio_pread(const uint64_t tenant_id,
                const ObTmpFileIOInfo &io_info,
                const int64_t offset,
                ObTmpFileIOHandle &io_handle,
                ObITmpFileHandle* file_handle = nullptr);

  int read(const uint64_t tenant_id,
           const ObTmpFileIOInfo &io_info,
           ObTmpFileIOHandle &io_handle,
           ObITmpFileHandle* file_handle = nullptr);

  int pread(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            const int64_t offset,
            ObTmpFileIOHandle &io_handle,
            ObITmpFileHandle* file_handle = nullptr);
  // NOTE:
  //   only support append write.
  int write(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            ObITmpFileHandle* file_handle = nullptr);

  int truncate(const uint64_t tenant_id,
               const int64_t fd,
               const int64_t offset,
               ObITmpFileHandle* file_handle = nullptr);

  int seal(const uint64_t tenant_id,
           const int64_t fd,
           ObITmpFileHandle* file_handle = nullptr);

  int get_tmp_file_size(const uint64_t tenant_id,
                        const int64_t fd,
                        int64_t &file_size,
                        ObITmpFileHandle* file_handle = nullptr);

  int get_tmp_file(const uint64_t tenant_id, const int64_t fd, ObITmpFileHandle &handle);
  int get_tmp_file_disk_usage(const uint64_t tenant_id, int64_t &disk_data_size, int64_t &occupied_disk_size);
  int get_tmp_file_fds(const uint64_t tenant_id, ObIArray<int64_t> &fd_arr);
  int get_tmp_file_info(const uint64_t tenant_id, const int64_t fd, ObTmpFileBaseInfo *tmp_file_info);
  void set_compressible_info(const uint64_t tenant_id,
                             const int64_t fd,
                             const OB_TMP_FILE_TYPE file_type,
                             const int64_t compressible_fd,
                             const void* compressible_file);
};

#define FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH (::oceanbase::tmp_file::ObTenantTmpFileManagerWithMTLSwitch::get_instance())

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_MANAGER_H_
