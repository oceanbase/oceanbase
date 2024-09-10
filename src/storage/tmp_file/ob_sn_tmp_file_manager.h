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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_eviction_manager.h"
#include "storage/tmp_file/ob_tmp_file_page_cache_controller.h"

namespace oceanbase
{
namespace tmp_file
{
struct ObTmpFileKey final
{
  explicit ObTmpFileKey(const int64_t fd) : fd_(fd) {}
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = murmurhash(&fd_, sizeof(int64_t), 0);
    return OB_SUCCESS;
  }
  OB_INLINE bool operator==(const ObTmpFileKey &other) const { return fd_ == other.fd_; }
  TO_STRING_KV(K(fd_));
  int64_t fd_;
};

class ObSNTenantTmpFileManager final
{
public:
  typedef common::ObLinearHashMap<ObTmpFileKey, ObTmpFileHandle> TmpFileMap;
public:
  ObSNTenantTmpFileManager();
  ~ObSNTenantTmpFileManager();
  static int mtl_init(ObSNTenantTmpFileManager *&manager);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int alloc_dir(int64_t &dir_id);
  int open(int64_t &fd, const int64_t &dir_id, const char* const label = nullptr);
  int remove(const int64_t fd);

  void refresh_meta_memory_limit();

public:
  int aio_read(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle);
  int aio_pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObSNTmpFileIOHandle &io_handle);
  int read(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle);
  int pread(const ObTmpFileIOInfo &io_info, const int64_t offset, ObSNTmpFileIOHandle &io_handle);
  // NOTE:
  //   only support append write.
  int aio_write(const ObTmpFileIOInfo &io_info, ObSNTmpFileIOHandle &io_handle);
  // NOTE:
  //   only support append write.
  int write(const ObTmpFileIOInfo &io_info);
  int truncate(const int64_t fd, const int64_t offset);

public:
  int get_tmp_file(const int64_t fd, ObTmpFileHandle &file_handle);
  int get_tmp_file_size(const int64_t fd, int64_t &size);
  int get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list);
  int get_macro_block_count(int64_t &macro_block_count);
  OB_INLINE ObIAllocator * get_callback_allocator() { return &callback_allocator_; }
  OB_INLINE ObTmpFileBlockManager &get_tmp_file_block_manager() { return tmp_file_block_manager_; }
  OB_INLINE ObTmpFilePageCacheController &get_page_cache_controller() { return page_cache_controller_; }

public:
  //for virtual table to show
  int get_tmp_file_fds(ObIArray<int64_t> &fd_arr);
  int get_tmp_file_info(const int64_t fd, ObSNTmpFileInfo &tmp_file_info);
private:
  class CollectTmpFileKeyFunctor final
  {
  public:
    CollectTmpFileKeyFunctor(ObIArray<int64_t> &fds)
        : fds_(fds) {}
    bool operator()(const ObTmpFileKey &key, const ObTmpFileHandle &tmp_file_handle);

  private:
    ObIArray<int64_t> &fds_;
  };

private:
  static const int64_t REFRESH_CONFIG_INTERVAL = 5 * 60 * 1000 * 1000L; // 5min
  static const int64_t META_DEFAULT_LIMIT = 15 * 1024L * 1024L * 1024L;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t last_access_tenant_config_ts_;
  int64_t last_meta_mem_limit_;
  common::ObConcurrentFIFOAllocator tmp_file_allocator_;
  common::ObFIFOAllocator callback_allocator_;
  common::ObFIFOAllocator wbp_index_cache_allocator_;
  common::ObFIFOAllocator wbp_index_cache_bucket_allocator_;
  TmpFileMap files_;
  ObTmpFileBlockManager tmp_file_block_manager_;
  ObTmpFilePageCacheController page_cache_controller_;

  int64_t current_fd_;
  int64_t current_dir_id_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_
