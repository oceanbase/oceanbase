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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_MANAGER_H_

#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_compress_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_compress_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileKey;

class ObTenantCompTmpFileManagerWithMTLSwitch final
{
public:
  static ObTenantCompTmpFileManagerWithMTLSwitch &get_instance();
  int alloc_dir(const uint64_t tenant_id, int64_t &dir_id);
  //TODO: comptype default value -> NONE_COMPRESSOR
  int open(const uint64_t tenant_id,
           int64_t &fd,
           const int64_t dir_id,
           const char* const label = nullptr,
           const ObCompressorType comptype = NONE_COMPRESSOR,
           const int64_t comp_unit_size = 0,
           const int64_t dop = 1  /* parallelism hint */);
  int remove(const uint64_t tenant_id, const int64_t fd);

public:
  // int aio_read(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObCompTmpFileIOHandle &io_handle);
  // int aio_pread(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, const int64_t offset, ObCompTmpFileIOHandle &io_handle);
  int read(const uint64_t tenant_id,
           const ObTmpFileIOInfo &io_info,
           ObCompTmpFileIOHandle &io_handle,
           ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  // int pread(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, const int64_t offset, ObCompTmpFileIOHandle &io_handle);
  // NOTE:
  //   only support append write.
  int write(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  // int truncate(const uint64_t tenant_id, const int64_t fd, const int64_t offset);
  // Mark file writing end
  int seal(const uint64_t tenant_id,
           const int64_t fd,
           ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  int get_tmp_file_size(const uint64_t tenant_id,
                        const int64_t fd, int64_t &file_size,
                        ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  int get_tmp_file_bunch(const uint64_t tenant_id, const int64_t fd, ObCompTmpFileBunchHandle &file_bunch_handle);
};

class ObTenantCompressTmpFileManager final
{
public:
  typedef common::ObLinearHashMap<ObTmpFileKey, ObCompressTmpFileHandle> CompTmpFileMap;
  typedef common::ObLinearHashMap<ObTmpFileKey, ObCompTmpFileBunchHandle> CompTmpFileBunchMap;
public:
  struct FileSealController
  {
    FileSealController()
      : total_unsealed_comp_file_cnt_(0),
        cur_file_bunch_ptr_(nullptr),
        unsealed_bunch_list_(),
        lock_(common::ObLatchIds::TMP_FILE_LOCK) {}
    OB_INLINE void reset()
    {
      total_unsealed_comp_file_cnt_ = 0;
      cur_file_bunch_ptr_ = nullptr;
      unsealed_bunch_list_.reset();
    }
    void destroy();
    int add_bunch_to_list(const uint64_t tenant_id, const int64_t dop, const int64_t suggest_unsealed_file_num, ObCompressTmpFileBunch *file_bunch_ptr, int64_t &new_file_num);
    int remove_bunch_from_list(ObCompressTmpFileBunch *file_bunch_ptr, const int64_t file_num);
    int seal_partial_comp_tmp_files_(const uint64_t tenant_id,
                                     const int64_t max_file_num_each_bunch,
                                     const int64_t total_need_seal_file_num,
                                     int64_t &actual_seal_file_num);

    int64_t total_unsealed_comp_file_cnt_;
    ObCompressTmpFileBunch *cur_file_bunch_ptr_;
    ObDList<ObCompressTmpFileBunch> unsealed_bunch_list_;
    ObSpinLock lock_;
  };
public:
  ObTenantCompressTmpFileManager():
    is_inited_(false),
    seal_controller_() {}
  ~ObTenantCompressTmpFileManager() { destroy(); }
  static int mtl_init(ObTenantCompressTmpFileManager *&manager);
  int init();
  int start() { return OB_SUCCESS; }
  void stop() {}
  void wait() {}
  void destroy();

  int alloc_dir(const uint64_t tenant_id, int64_t &dir_id);
  int open(const uint64_t tenant_id,
           int64_t &fd,
           const int64_t dir_id,
           const char* const label,
           const ObCompressorType comptype,
           const int64_t comp_unit_size,
           const int64_t dop);
  int remove(const uint64_t tenant_id, const int64_t fd);

public:
  // int aio_read(const uint64_t tenant_id, const ObTmpFileIOInfo &io_info, ObCompTmpFileIOHandle &io_handle);
  int read(const uint64_t tenant_id,
           const ObTmpFileIOInfo &io_info,
           ObCompTmpFileIOHandle &io_handle,
           ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);
  // NOTE:
  //   only support append write.
  int write(const uint64_t tenant_id,
            const ObTmpFileIOInfo &io_info,
            ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  // Mark file writing end
  int seal(const uint64_t tenant_id,
           const int64_t fd,
           ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  int get_tmp_file_size(const uint64_t tenant_id,
                        const int64_t fd, int64_t &file_size,
                        ObCompTmpFileBunchHandle* file_bunch_handle = nullptr);

  int get_tmp_file(const int64_t fd, ObCompressTmpFileHandle &comp_file_handle);
  int get_tmp_file_bunch(const int64_t fd, ObCompTmpFileBunchHandle &file_bunch_handle);

private:
  int cal_tenant_tmp_file_bunch_parallel_num_(const uint64_t tenant_id,
                                              const int64_t dop,
                                              ObCompressTmpFileBunch *comp_tmp_file_bunch,
                                              int64_t& parallel_num);

  int get_tmp_file_bunch_(const int64_t fd,
                          ObCompTmpFileBunchHandle* file_bunch_handle_hint,
                          ObCompTmpFileBunchHandle*& file_bunch_handle_ptr,
                          ObCompTmpFileBunchHandle &file_bunch_handle);

private:
  bool is_inited_;
  common::ObConcurrentFIFOAllocator comp_tmp_file_allocator_;
  common::ObFIFOAllocator compress_buf_allocator_;
  common::ObConcurrentFIFOAllocator comp_file_bunch_allocator_;
  CompTmpFileMap compressible_files_;
  CompTmpFileBunchMap comp_file_bunches_;
  FileSealController seal_controller_;
};

#define COMP_FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH (::oceanbase::tmp_file::ObTenantCompTmpFileManagerWithMTLSwitch::get_instance())

}
}

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_COMPRESS_TMP_FILE_MANAGER_H_
