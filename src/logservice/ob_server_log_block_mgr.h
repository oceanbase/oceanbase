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

#ifndef OCEANBASE_LOGSERVICE_OB_SERVER_LOG_BLOCK_MGR_
#define OCEANBASE_LOGSERVICE_OB_SERVER_LOG_BLOCK_MGR_

#include <fcntl.h>                          // O_RDWR
#include "lib/lock/ob_spin_lock.h"          // ObSpinLock
#include "lib/container/ob_se_array.h"      // ObSEArray
#include "lib/utility/ob_macro_utils.h"     // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"     // TOS_TRING_KV
#include "lib/ob_define.h"                  // OB_MAX_FILE_NAME_LENGTH
#include "lib/lock/ob_tc_rwlock.h"          // ObTCRWLock
#include "lib/lock/ob_spin_lock.h"          // ObSpinLock
#include "lib/function/ob_function.h"       // ObFunction
#include "palf/log_define.h"                // block_id_t
#include "palf/log_block_pool_interface.h"  // ObIServerLogBlockPool
#include "palf/log_io_utils.h"              // ObBaseDirFunctor

namespace oceanbase
{
namespace logservice
{
class ObLogService;
class ObServerLogBlockMgr : public palf::ILogBlockPool
{
public:
  static int check_clog_directory_is_empty(const char *clog_dir, bool &result);
private:
  static const char *LOG_POOL_PATH;
  static const int64_t DEFAULT_BLOCK_CNT = 10;
  static const int64_t GB = 1024 * 1024 * 1024ll;
  static const int64_t MB = 1024 * 1024ll;
  static const int64_t KB = 1024ll;
  static const int64_t BLOCK_SIZE = palf::PALF_PHY_BLOCK_SIZE;
  static const int64_t LOG_POOL_META_SERIALIZE_SIZE = 4 * KB;
  static const int64_t SLEEP_TS_US = 1 * 1000;
  static const int CREATE_FILE_FLAG = O_RDWR | O_CREAT | O_EXCL | O_SYNC | O_DIRECT;
  static const int OPEN_FILE_FLAG = O_RDWR | O_SYNC | O_DIRECT;
  static const int OPEN_DIR_FLAG = O_DIRECTORY | O_RDONLY;
  static const int CREATE_DIR_MODE =
      S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH;
  static const int CREATE_FILE_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int64_t RUN_INTERVAL = 1 * 1000 * 1000;

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef ObSEArray<palf::block_id_t, DEFAULT_BLOCK_CNT> BlockIdArray;

private:
  static const int64_t NORMAL_STATUS;
  static const int64_t EXPANDING_STATUS;
  static const int64_t SHRINKING_STATUS;
  struct LogPoolMeta
  {
    LogPoolMeta(const int64_t curr_total_size, const int64_t next_total_size,
                const int64_t status)
        : curr_total_size_(curr_total_size),
          next_total_size_(next_total_size),
          status_(status)
    {}

    LogPoolMeta() : curr_total_size_(0), next_total_size_(0), status_(NORMAL_STATUS)
    {}

    ~LogPoolMeta()
    {
      reset();
    }

    bool operator==(const LogPoolMeta &rhs) const
    {
      return curr_total_size_ == rhs.curr_total_size_
             && next_total_size_ == rhs.next_total_size_ && status_ == rhs.status_;
    }
    void reset();
    bool resizing() const;
    int64_t curr_total_size_;
    int64_t next_total_size_;
    int64_t status_;
    TO_STRING_KV(K_(curr_total_size), K_(next_total_size), K_(status));
    NEED_SERIALIZE_AND_DESERIALIZE;

  };

  struct LogPoolMetaEntry {
    LogPoolMetaEntry() {}
    LogPoolMetaEntry(const LogPoolMeta &log_pool_meta)
      : magic_(MAGIC_NUM),
        version_(VERSION),
        flag_(0),
        log_pool_meta_(log_pool_meta) {}
    ~LogPoolMetaEntry() {}
    void update_checksum();
    bool check_integrity();
    int16_t magic_;
    int16_t version_;
    int32_t flag_;
    LogPoolMeta log_pool_meta_;
    int64_t checksum_;
    TO_STRING_KV(K_(magic), K_(version), K_(flag), K_(log_pool_meta), K_(checksum));
    NEED_SERIALIZE_AND_DESERIALIZE;
  private:
    int64_t calc_checksum_();
    static const int64_t MAGIC_NUM = 0x4C50;
    static const int64_t VERSION = 1;
  };

  struct GetBlockIdListFunctor : public palf::ObBaseDirFunctor
  {
    GetBlockIdListFunctor()
    {}
    virtual ~GetBlockIdListFunctor()
    {}
    int func(const dirent *entry);

    BlockIdArray block_id_array_;
  };

public:
  ObServerLogBlockMgr();
  ~ObServerLogBlockMgr();
  // @brief initialize ObServerLogBlockMgr, reload myself if has reserved.
  // @param[in] the path of log disk(ie: like **/store/clog), and make sure this path has existed.
  // @retval
  //   OB_SUCCESS
  //   OB_IO_ERROR
  int init(const char *log_disk_base_path);
  int start(const int64_t log_disk_size);
  void destroy();
  bool is_reserved() const;


  // @brief adjust the total disk space contains by ObServerLogBlockMgr.
  //
  // get 'log_disk_size' from GCONF.
  //
  // This is an atomic operation.
  // @retval
  //   OB_SUCCESS
  //   OB_NOT_SUPPORTED, 'log_disk_size' is smaller than 1GB
  //   OB_ALLOCATE_DISK_SPACE_FAILED, no space left on device
  int try_resize();

  // @brief get current disk usage.
  // @param[out] current in used diskspace.
  // @param[out] current total diskspace.
  int get_disk_usage(int64_t &in_use_size_byte, int64_t &total_size_byte);

  // @brief allocate a new block, and move it to the specified directory with the specified
  // name.
  // @param[in] the file description of directory.
  // @param[in] the name of this block.
  // @param[in] specified block size.
  int create_block_at(const palf::FileDesc &dest_dir_fd,
                      const char *dest_block_path,
                      const int64_t block_size) override final;

  // @brief recycle a block, and move it to myself directory.
  // @param[in] the directory description of source directory.
  // @param[in] the name of this block.
  int remove_block_at(const palf::FileDesc &src_dir_fd,
                      const char *src_block_path) override final;

  // @brief before 'create_tenant' in ObMultiTenant, need allocate log disk size firstly.
  // @param[in] the log disk size need by tenant.
  // @return
  //   OB_SUCCESS
  //   OB_MACHINE_RESOURCE_NOT_ENOUGH
  // NB: accurately, when tenant has existed in 'omt_', we can create it in ObServerLogBlockMgr
  int create_tenant(const int64_t log_disk_size);

  // @brief after 'create_tenant' failed in ObMultiTenant, need deallocate log disk size.
  // @param[in] the log disk size used by tenant.
  void abort_create_tenant(const int64_t log_disk_size);

  // @brief before 'update_tenant_log_disk_size' in ObMultiTenant, need update it.
  // @param[in] the log disk size used by tenant.
  // @param[in] the log disk size need by tenant.
  // @param[in] the log disk size allowed by tenant
  // @param[in] ObLogService*
  //   OB_SUCCESS
  //   OB_MACHINE_RESOURCE_NOT_ENOUGH
  int update_tenant(const int64_t old_log_disk_size,
                    const int64_t new_log_disk_size,
                    int64_t &allowed_log_disk_size,
                    ObLogService *log_service);

  // @brief after 'del_tenant' in ObMultiTenant success, need remove it from ObServerLogBlockMgr
  // NB: accurately, when tenant not exist in 'omt_', we can remove it from ObServerLogBlockMgr
  int remove_tenant(const int64_t log_disk_size);

  TO_STRING_KV("dir:",
               log_pool_path_, K_(dir_fd), K_(meta_fd), K_(log_pool_meta),
               K_(min_block_id), K_(max_block_id), K(min_log_disk_size_for_all_tenants_),
               K_(is_inited));

private:
  // @brief adjust the total disk space contains by ObServerLogBlockMgr.
  // @param[in] the new disk size of ObServerLogBlockMgr.
  //
  // If 'new_size_byte' is greater than 'curr_total_size_', do expand,
  // otherwise, do shrink.
  //
  // This is an atomic operation.
  // @retval
  //   OB_SUCCESS
  //   OB_NOT_SUPPORTED, 'new_size_byte' is smaller than 1GB
  //   OB_ALLOCATE_DISK_SPACE_FAILED, no space left on device
  int resize_(const int64_t new_size_byte);

  int do_init_(const char *log_pool_base_path);
  int prepare_dir_and_create_meta_(const char *log_pool_path,
                                   const char *log_pool_tmp_path);
  int do_load_(const char *log_disk_path);
  int scan_log_disk_dir_(const char *log_disk_path, int64_t &has_allocated_block_cnt);
  int scan_log_pool_dir_and_do_trim_();
  int trim_log_pool_dir_and_init_block_id_range_(const BlockIdArray &block_id_array,
                                                 const int64_t first_need_trim_idx);
  int try_continous_to_resize_(const int64_t has_allocated_block_size);
  int load_meta_();
  bool check_log_pool_whehter_is_integrity_(const int64_t has_allocated_block_size);

  bool check_space_is_enough_(const int64_t log_disk_size) const;
  int get_all_tenants_log_disk_size_(int64_t &log_disk_size) const;
private:
  int update_log_pool_meta_guarded_by_lock_(const LogPoolMeta &meta);
  const LogPoolMeta &get_log_pool_meta_guarded_by_lock_() const;
  int64_t get_total_size_guarded_by_lock_();
  int64_t get_free_size_guarded_by_lock_();
  int64_t get_in_use_size_guarded_by_lock_();
  int get_and_inc_max_block_id_guarded_by_lock_(palf::block_id_t &out_block_id,
                                                const bool remove_block=false);
  int get_and_inc_min_block_id_guarded_by_lock_(palf::block_id_t &out_block_id,
                                                const bool create_block=false);
  int move_block_not_guarded_by_lock_(const palf::FileDesc &dest_dir_fd,
                                      const char *dest_block_path,
                                      const palf::FileDesc &src_dir_fd,
                                      const char *src_block_path);
  int fsync_after_rename_(const palf::FileDesc &dest_dir_fd);
  int make_resizing_tmp_dir_(const char *dir_path, palf::FileDesc &out_dir_fd);
  int remove_resizing_tmp_dir_(const char *dir_path, const palf::FileDesc &in_dir_fd);
  int do_resize_(const LogPoolMeta &old_log_pool_meta, const int64_t resize_block_cnt,
                 LogPoolMeta &new_log_pool_meta);

  // 原子性的保证:
  // 1. 临时的扩容目录
  // 2. 移动失败会重试
  int do_expand_(const LogPoolMeta &new_log_meta, const int64_t resize_block_cnt);

  // 原子性的保证:
  // 1. 删除操作失败会重试
  int do_shrink_(const LogPoolMeta &new_log_meta, const int64_t resize_block_cnt);
  int allocate_blocks_at_tmp_dir_(const palf::FileDesc &dir_fd,
                                  const palf::block_id_t start_block_id,
                                  const int64_t block_cnt);
  int allocate_block_at_tmp_dir_(const palf::FileDesc &dir_fd, const palf::block_id_t block_id);
  int free_blocks_at_log_pool_(const int64_t block_cnt);
  int free_block_at_(const palf::FileDesc &dir_fd, const palf::block_id_t block_id);
  int move_blocks_from_tmp_dir_to_log_pool_(const palf::FileDesc &dir_fd,
                                            const palf::block_id_t start_block_id,
                                            const int64_t block_cnt);
  int64_t calc_block_cnt_by_size_(const int64_t curr_total_size);
  int get_has_allocated_blocks_cnt_in_(const char *log_disk_path,
                                       int64_t &has_allocated_block_cnt);
  int64_t lower_align_(const int64_t new_size_byte);
  int remove_tmp_file_or_directory_for_tenant_(const char *log_disk_path);
private:
  int open_until_success_(const char *src_block_path, const int flag,
                          palf::FileDesc &out_fd);
  int fallocate_until_success_(const palf::FileDesc &src_fd, const int64_t block_size);
  int unlinkat_until_success_(const palf::FileDesc &src_dir_fd, const char *block_path,
                              const int flag);
  int fsync_until_success_(const palf::FileDesc &src_fd);
  int renameat_until_success_(const palf::FileDesc &dest_dir_fd,
                              const char *dest_block_path,
                              const palf::FileDesc &src_dir_fd,
                              const char *src_block_path);
  int write_unitl_success_(const palf::FileDesc &dest_fd, const char *src_buf,
                           const int64_t src_buf_len, const int64_t offset);
  int read_unitl_success_(const palf::FileDesc &src_fd, char *dest_buf,
                          const int64_t dest_buf_len, const int64_t offset);
private:
  typedef common::ObFunction<int(int64_t&)> GetTenantsLogDiskSize;
  mutable ObSpinLock log_pool_meta_lock_;
  mutable ObSpinLock resize_lock_;
  mutable RWLock block_id_range_lock_;
  char log_pool_path_[OB_MAX_FILE_NAME_LENGTH];
  char *log_pool_meta_serialize_buf_;
  palf::FileDesc dir_fd_;
  palf::FileDesc meta_fd_;
  LogPoolMeta log_pool_meta_;
  // [min_block_id_, max_block_id_)
  palf::block_id_t min_block_id_;
  palf::block_id_t max_block_id_;
  // minimum log disk size to hold all tenants on this server, just a memory value.
  //
  // In start(), need update it with 'get_all_tenants_log_disk_size_'
  // In create_tenant, need update it:
  //  1. inc it with new tenant log disk size.
  // In abort_create_tenant, need update it:
  //  1. dec it with new tenant log disk size.
  // In update_tenant, need update it:
  //  1. dec old log disk size of tenant;
  //  2, inc new log disk size of tenant.
  // In abort_update_tenant, need update it
  //  1. dec new log disk size of tenant;
  //  2, inc old log disk size of tenant.
  // In remove_tenant, need update it with 'get_all_tenants_log_disk_size_'.
  //
  // After restart, all tenants which need exist in 'omt_' has been loaded,
  // and then, we cal init 'min_log_disk_size_for_all_tenants_' via
  // get_all_tenants_log_disk_size_(int64_t) in start().
  //
  // NB: before start(), can not execute resize, and the initial value of
  // 'min_log_disk_size_for_all_tenants_' is 0.
  int64_t min_log_disk_size_for_all_tenants_;
  GetTenantsLogDiskSize get_tenants_log_disk_size_func_;
  // NB: in progress of expanding, the free size byte calcuated by BLOCK_SIZE * (max_block_id_ - min_block_id_) may be greater than
  //     curr_total_size_, if we calcuated log disk in use by curr_total_size_ - 'free size byte', the resule may be negative.
  int64_t block_cnt_in_use_;
  // Before start ObServerLogBlockMgr, not support log disk resize.
  bool is_started_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerLogBlockMgr);
};
} // namespace logservice
} // namespace oceanbase
#endif
