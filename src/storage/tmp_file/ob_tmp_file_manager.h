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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/blocksstable/ob_ss_tmp_file_manager.h"
#include "storage/tmp_file/ob_sn_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTenantTmpFileManager final
{
public:
  ObTenantTmpFileManager(): is_inited_(false) {}
  ~ObTenantTmpFileManager() { destroy(); }
  static int mtl_init(ObTenantTmpFileManager *&manager);
  static ObTenantTmpFileManager &get_instance();
  ObSNTenantTmpFileManager &get_sn_file_manager() { return sn_file_manager_; }
  blocksstable::ObSSTenantTmpFileManager &get_ss_file_manager() { return ss_file_manager_; }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int alloc_dir(int64_t &dir_id);
  int open(int64_t &fd, const int64_t &dir_id, const char* const label = nullptr);
  int remove(const int64_t fd);

public:
  int aio_read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle);
  int aio_pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &io_handle);
  int read(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle);
  int pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObTmpFileIOHandle &io_handle);
  // NOTE:
  //   only support append write.
  int aio_write(const ObTmpFileIOInfo &io_info, ObTmpFileIOHandle &io_handle);
  // NOTE:
  //   only support append write.
  int write(const ObTmpFileIOInfo &io_info);
  int truncate(const int64_t fd, const int64_t offset);
  int get_tmp_file_size(const int64_t fd, int64_t &file_size);
public:
  //for virtual table to show
  int get_tmp_file_fds(ObIArray<int64_t> &fd_arr);
  int get_tmp_file_info(const int64_t fd, ObSNTmpFileInfo &tmp_file_info);
private:
  bool is_inited_;
  ObSNTenantTmpFileManager sn_file_manager_;
  blocksstable::ObSSTenantTmpFileManager ss_file_manager_;
};

#define FILE_MANAGER_INSTANCE_V2 (::oceanbase::tmp_file::ObTenantTmpFileManager::get_instance())
}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_MANAGER_H_
