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

#include "ob_server_log_block_mgr.h"
#include <fcntl.h>                              // IO operation
#include <type_traits>                          // decltype
#include <regex>                                // std::regex
#include "lib/lock/ob_spin_lock.h"              // ObSpinLockGuard
#include "lib/ob_define.h"                      // OB_MAX_FILE_NAME_LENGTH
#include "lib/time/ob_time_utility.h"           // ObTimeUtility
#include "lib/utility/ob_macro_utils.h"         // OB_UNLIKELY
#include "lib/utility/ob_utility.h"             // lower_align
#include "lib/utility/serialization.h"          // serialization
#include "lib/file/file_directory_utils.h"      // FileDirectoryUtils
#include "lib/checksum/ob_crc64.h"              // ob_crc64
#include "lib/utility/utility.h"                // ObTimeGuard
#include "lib/container/ob_se_array_iterator.h" // ObSEArrayIterator
#include "lib/thread/ob_thread_name.h"          // set_thread_name
#include "palf/log_block_pool_interface.h"      // ILogBlockPool
#include "palf/log_define.h"                    // block_id_to_string
#include "observer/ob_server.h"                 // OBSERVER
#include "observer/ob_server_utils.h"           // get_log_disk_info_in_config
#include "share/unit/ob_unit_resource.h"        // UNIT_MIN_LOG_DISK_SIZE
#include "share/ob_errno.h"                     // errno
#include "logservice/ob_log_service.h"          // ObLogService
#include "logservice/palf/log_io_utils.h"       // renameat_with_retry
namespace oceanbase
{
using namespace palf;
using namespace share;
namespace logservice
{
const char *ObServerLogBlockMgr::LOG_POOL_PATH = "log_pool";
const int64_t ObServerLogBlockMgr::NORMAL_STATUS = 0;
const int64_t ObServerLogBlockMgr::EXPANDING_STATUS = 1;
const int64_t ObServerLogBlockMgr::SHRINKING_STATUS = 2;

int ObServerLogBlockMgr::check_clog_directory_is_empty(const char *clog_dir, bool &result)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  int64_t num = 0;
  result = false;
  if (NULL == clog_dir) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "directory path is NULL, ", K(ret));
  } else if (NULL == (dir = opendir(clog_dir))) {
    ret = OB_ERR_SYS;
    CLOG_LOG(WARN, "Fail to open dir, ", K(ret), K(errno), K(clog_dir));
  } else {
    result = true;
    while (NULL != (entry = readdir(dir))) {
      if (0 != strcmp(entry->d_name, ".") && 0 != strcmp(entry->d_name, "..")
          && 0 != strcmp(entry->d_name, ObServerLogBlockMgr::LOG_POOL_PATH)) {
        result = false;
      }
    }
  }

  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

ObServerLogBlockMgr::ObServerLogBlockMgr()
    : log_pool_meta_serialize_buf_(NULL),
      dir_fd_(-1),
      meta_fd_(-1),
      log_pool_meta_(),
      min_block_id_(0),
      max_block_id_(0),
      min_log_disk_size_for_all_tenants_(0),
      block_cnt_in_use_(0),
      is_started_(false),
      is_inited_(false)
{
  memset(log_pool_path_, '\0', OB_MAX_FILE_NAME_LENGTH);
}

ObServerLogBlockMgr::~ObServerLogBlockMgr()
{
  destroy();
}

int ObServerLogBlockMgr::init(const char *log_disk_base_path)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObServerLogBlockMgr inited twice", K(ret), KPC(this));
  } else if (OB_ISNULL(log_disk_base_path)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument", K(ret), KPC(this), KP(log_disk_base_path));
  } else if (OB_FAIL(do_init_(log_disk_base_path))) {
    CLOG_LOG(ERROR, "do_init_ failed", K(ret), KPC(this), K(log_disk_base_path));
  } else if (OB_FAIL(do_load_(log_disk_base_path))) {
    CLOG_LOG(ERROR, "do_load_ failed", K(ret), KPC(this), K(log_disk_base_path));
  } else {
    get_tenants_log_disk_size_func_ = [this](int64_t &log_disk_size) -> int
    {
      log_disk_size = 0;
      return get_all_tenants_log_disk_size_(log_disk_size);
    };
    CLOG_LOG(INFO, "ObServerLogBlockMgr init success", KPC(this));
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObServerLogBlockMgr::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObServerLogBlockMgr  destroy", KPC(this));
  is_inited_ = false;
  is_started_ = false;
  block_cnt_in_use_ = 0;
  min_log_disk_size_for_all_tenants_ = 0;
  max_block_id_ = 0;
  min_block_id_ = 0;
  log_pool_meta_.reset();
  if (true == is_valid_file_desc(meta_fd_)) {
    ::close(meta_fd_);
    meta_fd_ = -1;
  }
  if (true == is_valid_file_desc(dir_fd_)) {
    ::close(dir_fd_);
    dir_fd_ = -1;
  }
  if (NULL != log_pool_meta_serialize_buf_) {
    ob_free_align(log_pool_meta_serialize_buf_);
    log_pool_meta_serialize_buf_ = NULL;
  }
  memset(log_pool_path_, '\0', OB_MAX_FILE_NAME_LENGTH);
}

bool ObServerLogBlockMgr::is_reserved() const
{
  ObSpinLockGuard guard(log_pool_meta_lock_);
  return 0 != log_pool_meta_.curr_total_size_;
}

int ObServerLogBlockMgr::start(const int64_t new_size_byte)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObServerLogBlockMGR is not inited", K(ret), KPC(this));
  } else if (!check_space_is_enough_(new_size_byte)) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    CLOG_LOG(WARN, "server log disk is too small to hold all tenants or the count of tenants"
             ", log disk space is not enough!!!",
             K(ret), KPC(this), K(min_log_disk_size_for_all_tenants_), K(new_size_byte));
  } else if (OB_FAIL(resize_(new_size_byte))) {
    CLOG_LOG(ERROR, "resize failed", K(ret), KPC(this));
  } else {
    ATOMIC_STORE(&is_started_, true);
    CLOG_LOG(INFO, "ObServerLogBlockMGR start success", K(ret), KPC(this), K(new_size_byte));
  }
  return ret;
}

int ObServerLogBlockMgr::resize_(const int64_t new_size_byte)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObSpinLockGuard guard(resize_lock_);
  LogPoolMeta old_log_pool_meta = get_log_pool_meta_guarded_by_lock_();
  LogPoolMeta new_log_pool_meta = old_log_pool_meta;
  const int64_t aligned_new_size_byte = new_log_pool_meta.next_total_size_ = lower_align_(new_size_byte);
  const int64_t curr_total_size = old_log_pool_meta.curr_total_size_;
  const int64_t old_block_cnt = calc_block_cnt_by_size_(curr_total_size);
  const int64_t new_block_cnt =
      calc_block_cnt_by_size_(new_log_pool_meta.next_total_size_);
  const int64_t resize_block_cnt = std::abs(new_block_cnt - old_block_cnt);
  const int64_t free_size_byte = get_free_size_guarded_by_lock_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObServerLogBlockMgr has not inited", K(ret), KPC(this),
             K(new_size_byte), K(aligned_new_size_byte));
  } else if (curr_total_size == aligned_new_size_byte) {
    CLOG_LOG(TRACE, "no need do resize", K(ret), KPC(this), K(new_size_byte), K(aligned_new_size_byte));
  } else if (FALSE_IT(new_log_pool_meta.status_ =
                          (aligned_new_size_byte > curr_total_size ? EXPANDING_STATUS
                                                           : SHRINKING_STATUS))) {
  } else if (aligned_new_size_byte < min_log_disk_size_for_all_tenants_) {
    ret = OB_NOT_SUPPORTED;
    LOG_DBA_ERROR(OB_NOT_SUPPORTED,
                  "possible reason",
                  "new log_disk_size is not enough to hold all tenants, please check the configuration about log disk",
                  "new log disk size(MB)", (new_size_byte+1024*1024-1)/1024/1024,
                  "min log disk size(MB)", (min_log_disk_size_for_all_tenants_+1024*1024-1)/1024/1024);
  } else if (OB_FAIL(
                 do_resize_(old_log_pool_meta, resize_block_cnt, new_log_pool_meta))) {
    if (OB_ALLOCATE_DISK_SPACE_FAILED == ret) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_DBA_ERROR(OB_ALLOCATE_DISK_SPACE_FAILED,
                    "possible reason",
                    "may be diskspace is not enough, please check the configuration about log disk",
                    "new log disk size(MB)", (new_size_byte+1024*1024-1)/1024/1024);
    } else {
      CLOG_LOG(ERROR, "do_resize_ failed", K(ret), KPC(this), K(old_log_pool_meta),
               K(new_log_pool_meta));
    }
  } else {
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    CLOG_LOG(INFO, "resize success", K(ret), KPC(this), K(new_size_byte), K(aligned_new_size_byte),
             K(old_block_cnt), K(new_block_cnt), K(cost_ts));
  }
  return ret;
}

int ObServerLogBlockMgr::get_disk_usage(int64_t &in_use_size_byte, int64_t &total_size_byte)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObServerLogBlockMgr has not inited", K(ret), KPC(this));
  } else {
    total_size_byte = get_total_size_guarded_by_lock_();
    in_use_size_byte = get_in_use_size_guarded_by_lock_();
  }
  return ret;
}

int ObServerLogBlockMgr::create_block_at(const FileDesc &dest_dir_fd,
                                         const char *dest_block_path,
                                         const int64_t block_size)
{
  int ret = OB_SUCCESS;
  block_id_t src_block_id = LOG_INVALID_BLOCK_ID;
  char src_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObServerLogBlockMgr has not inited", K(ret), KPC(this));
  } else if (false == is_valid_file_desc(dest_dir_fd)
             || NULL == dest_block_path || BLOCK_SIZE != block_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument", K(ret), KPC(this), K(dest_dir_fd),
             K(dest_block_path), K(block_size));
  } else if (OB_FAIL(get_and_inc_min_block_id_guarded_by_lock_(src_block_id, true))) {
    CLOG_LOG(WARN, "get_and_inc_min_block_id_guarded_by_lock_ failed", K(ret), KPC(this),
             K(dest_dir_fd), K(dest_block_path));
  } else if (OB_FAIL(block_id_to_string(src_block_id, src_block_path, OB_MAX_FILE_NAME_LENGTH))) {
    CLOG_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(dest_block_path));
  } else if (OB_FAIL(move_block_not_guarded_by_lock_(dest_dir_fd, dest_block_path, dir_fd_,
                                                     src_block_path))) {
    CLOG_LOG(WARN, "move_block_not_guarded_by_lock_ failed", K(ret), KPC(this),
             K(dest_dir_fd), K(dest_block_path));
    // make sure the meta info of both directory has been flushed.
  } else if (OB_FAIL(fsync_after_rename_(dest_dir_fd))) {
    CLOG_LOG(ERROR, "fsync_after_rename_ failed", K(ret), KPC(this), K(dest_block_path),
             K(dest_dir_fd), K(src_block_path));
  } else {
    CLOG_LOG(INFO, "create_new_block_at success", K(ret), KPC(this), K(dest_dir_fd),
             K(dest_block_path));
  }
  return ret;
}

int ObServerLogBlockMgr::remove_block_at(const FileDesc &src_dir_fd,
                                         const char *src_block_path)
{
  int ret = OB_SUCCESS;
  block_id_t dest_block_id = LOG_INVALID_BLOCK_ID;
  char dest_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool result = true;
  if (OB_FAIL(is_block_used_for_palf(src_dir_fd, src_block_path, result))) {
    CLOG_LOG(ERROR, "block_is_used_for_palf failed", K(ret));
  } else if (false == result) {
    CLOG_LOG(ERROR, "this block is not used for palf", K(ret), K(src_block_path));
    ::unlinkat(src_dir_fd, src_block_path, 0);
  } else if (OB_FAIL(reuse_block_at(src_dir_fd, src_block_path))) {
    CLOG_LOG(ERROR, "reusle_block_at failed", K(ret), K(src_block_path));
  } else {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "ObServerLogBlockMGR has not inited", K(ret), KPC(this));
    } else if (OB_FAIL(get_and_inc_max_block_id_guarded_by_lock_(dest_block_id, true))) {
      CLOG_LOG(ERROR, "get_and_inc_max_block_id_guarded_by_lock_ failed", K(ret), KPC(this),
               K(src_dir_fd), K(src_block_path));
    } else if (OB_FAIL(block_id_to_string(dest_block_id, dest_block_path, OB_MAX_FILE_NAME_LENGTH))) {
      CLOG_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(dest_block_id));
    } else if (OB_FAIL(move_block_not_guarded_by_lock_(dir_fd_, dest_block_path, src_dir_fd,
                                                       src_block_path))) {
      CLOG_LOG(ERROR, "move_block_not_guarded_by_lock_ failed", K(ret), KPC(this),
               K(src_dir_fd), K(src_block_path));
      // make sure the meta info of both directory has been flushed.
    } else if (OB_FAIL(fsync_after_rename_(src_dir_fd))) {
      CLOG_LOG(ERROR, "fsync_after_rename_ failed", K(ret), KPC(this), K(dest_block_id),
               K(src_dir_fd), K(src_block_path));
    } else {
      CLOG_LOG(INFO, "delete_block_at success", K(ret), KPC(this), K(src_dir_fd),
               K(src_block_path));
    }
  }
  return ret;
}

int ObServerLogBlockMgr::create_tenant(const int64_t log_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObServerLogBlockMGR is not inited", K(log_disk_size), KPC(this));
  } else {
    ObSpinLockGuard guard(resize_lock_);
    int64_t tmp_log_disk_size = min_log_disk_size_for_all_tenants_;
    if ((tmp_log_disk_size += log_disk_size) > get_total_size_guarded_by_lock_()) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      CLOG_LOG(ERROR, "ObServerLogBlockMGR can not hold any new tenants",
          K(ret), KPC(this), K(log_disk_size));
    } else {
      min_log_disk_size_for_all_tenants_ = tmp_log_disk_size;
      CLOG_LOG(INFO, "ObServerLogBlockMGR create_tenant success", KPC(this), K(log_disk_size));
    }
  }
  return ret;
}

void ObServerLogBlockMgr::abort_create_tenant(const int64_t log_disk_size)
{
  if (IS_NOT_INIT) {
    CLOG_LOG_RET(WARN, OB_NOT_INIT, "ObServerLogBlockMGR is not inited", K(log_disk_size), KPC(this));
  } else {
    ObSpinLockGuard guard(resize_lock_);
    min_log_disk_size_for_all_tenants_ -= log_disk_size;
    OB_ASSERT(min_log_disk_size_for_all_tenants_ >= 0
        && min_log_disk_size_for_all_tenants_ <= get_total_size_guarded_by_lock_());
    CLOG_LOG(INFO, "ObServerLogBlockMGR abort_create_tenant success", KPC(this), K(log_disk_size));
  }
}

int ObServerLogBlockMgr::update_tenant(const int64_t old_log_disk_size,
                                       const int64_t new_log_disk_size,
                                       int64_t &allowed_new_log_disk_size,
                                       logservice::ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(resize_lock_);
  int64_t used_log_disk_size = 0, palf_log_disk_size = 0;
  bool can_update_log_disk_size_with_expected_log_disk = false;
  int64_t tmp_log_disk_size = min_log_disk_size_for_all_tenants_;
  tmp_log_disk_size -= old_log_disk_size;
  // 'old_log_disk_size' is current log disk size in ObTenant.
  // 'new_log_disk_size' is the latest log disk size record in __all_unit_config.
  // 'allowed_new_log_disk_size' is current allowed log disk size when update log disk.
  //
  // To avoid overselling, we can not use 'new_log_disk_size' to update unit config which will save in slog.
  // therefore, we need constuct a virtual log disk size which named with 'allowed_new_log_disk_size'.
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObServerLogBlockMGR is not inited", K(old_log_disk_size), K(new_log_disk_size), KPC(this));
  } else if (old_log_disk_size < 0 || new_log_disk_size < 0 || OB_ISNULL(log_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(old_log_disk_size), K(new_log_disk_size), KP(log_service), KPC(this));
  } else if (OB_FAIL(log_service->get_palf_stable_disk_usage(used_log_disk_size, palf_log_disk_size))) {
    CLOG_LOG(WARN, "fail to get_palf_stable_disk_usage", K(old_log_disk_size), K(new_log_disk_size));
    // The standard for determing whether it's in shrinking or expanding status:
    // 1. If 'palf_log_disk_size' is smaller than or equal to 'new_log_disk_size', it's in expanding status.
    // 2. If 'palf_log_disk_size' is greater than 'new_log_disk_size', it's in shrinking status.
    //
    // For shrinking log disk, we don't update ObTenantConfig of ObTenant to new ObTenantConfig until shrinking successfully.
    //
    // NB: All fields of new ObTenantConfig except log_disk_size has been updated in case of shrinking log disk.
    //
    // For example:
    // 1. before shrinkg log disk successfully, and then expand log disk.
    //    - At T1 timestamp, the original log disk is 100G, and update it to 50G, we will construct 'new_unit' with
    //      100G, but update palf with 50G because original log disk size is greater than new log disk size.
    //    - At T2 timestamp, the log disk size in current ObTenantConfig is 100G, and we update it to 80G, there are
    //      two scenarios:
    //      1. if 'palf_log_disk_size' which get from palf is 100G, we think palf is still in shrinking status. and we will
    //         construct 'new_unit' with 100G because 'palf_log_disk_size' is greater than new log disk size(80G). but udpate
    //         palf with 80G
    //      2. if 'palf_log_disk_size' which get from palf is 50G, we think palf has been in normal status. and we will
    //         construct 'new_unit' with 80G because 'palf_log_disk_size' is smaller than new log disk size(80G), but udpate
    //         palf with 80G.
    //
    // NB: when new_log_disk_size is zero(means convert real sys tenant to hidden sys tenant), need make allowed_new_log_disk_size
    //     to zero directlly, otherwise, there is no chance to update log disk size to zero because hidden sys tenant is invisible
    //     for ObTenantNodeBalancer, and min_log_disk_size_for_all_tenants_ can not be reduce.
  } else if (FALSE_IT(can_update_log_disk_size_with_expected_log_disk =
                      (new_log_disk_size >= palf_log_disk_size || 0 == new_log_disk_size))) {
    // For expanding log disk, we can update 'allowed_new_log_disk_size' to 'new_log_disk_size' directlly.
  } else if (can_update_log_disk_size_with_expected_log_disk && FALSE_IT(allowed_new_log_disk_size = new_log_disk_size)) {
    // For shrinking log disk, we still update log disk size of 'new_unit' to 'old_log_disk_size'.
  } else if (!can_update_log_disk_size_with_expected_log_disk && FALSE_IT(allowed_new_log_disk_size = old_log_disk_size)) {
  } else if ((tmp_log_disk_size += allowed_new_log_disk_size) > get_total_size_guarded_by_lock_()) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    CLOG_LOG(WARN, "ObServerLogBlockMGR can not hold any new tenants", KPC(this),  K(old_log_disk_size),
             K(new_log_disk_size), K(allowed_new_log_disk_size), K(tmp_log_disk_size));
    // case 1: for shrinking log disk, shrinking log disk from 100G to 50G.
    //         - At T1 timestamp, 'new_log_disk_size' is 50G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 100G.
    //           the log disk size record in slog is 100G, and we will update log disk size used for palf to 50G, but not update log
    //           disk which has assigned in ObServerLogBlockMGR.
    //         - At T2 timestamp, 'new_log_disk_size' is still 50G, 'old_log_disk_size' is still 100G, however, 'allowed_new_log_disk_size'
    //           is 50G because of shrinking log disk has been successfully, the log disk record in slog is 50G, and then we will update log disk
    //           size used for palf to 50G again but has no effect, log disk assigned in ObServerLogBlockMGR update to 50G(assume there is only one tenant).
    //
    // case 2: for expanding log disk, expanding log disk from 100G to 150G.
    //         - At T1 timestamp, 'new_log_disk_size' is 150G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 150G.
    //           the log disk size record in slog is 150G, and then, we will update log disk size used for palf to 150G, the log disk
    //           which has assigned in ObServerLogBlockMGR updaet to 150G(assume there is only one tenant).
    //
    // case 3: for shrinking log disk, shrinking log disk from 100G to 50G, and then shrinking log disk from 50G to 25G.
    //         - At T1 timestamp, 'new_log_disk_size' is 50G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 100G.
    //           the log disk size record in slog is 100G, and we will update log disk size used for palf to 50G, but not update log
    //           disk which has assigned in ObServerLogBlockMGR.
    //         - At T2 timestamp, 'new_log_disk_size' is 25G, 'old_log_disk_size' is still 100G, however, there are two possibility value for
    //           'allowed_new_log_disk_size':
    //           1. the value is 100G because of last shrinking log disk has not been successfully, the log disk record in slog is still 100G
    //              and then we will update log disk size used for palf to 25G, but not update log disk assigned in ObServerLogBlockMGR.
    //              At T3 timestamp, 'new_log_disk_size' is 25G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 25G,
    //              the log disk record in slog is 25G, the log disk assigned in ObServerLogBlockMGR is 25G.
    //           2. the value is 50G because of last shrinking log disk has been successfully, the log disk record in slog is 50G
    //              and then we will update log disk size used for palf to 25G, update log disk assigned in ObServerLogBlockMGR to 50G(assume there is only one tenant).
    //              At T3 timestamp, 'new_log_disk_size' is 25G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 25G,
    //              the log disk record in slog is 25G, the log disk assigned in ObServerLogBlockMGR is 25G.
    //
    // case 4: for shrinking log disk, shrinking log disk from 100G to 50G, and then expanding log disk from 50G to 80G.
    //         - At T1 timestamp, 'new_log_disk_size' is 50G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 100G.
    //           the log disk size record in slog is 100G, and we will update log disk size used for palf to 50G, but not update log
    //           disk which has assigned in ObServerLogBlockMGR.
    //         - At T2 timestamp, 'new_log_disk_size' is 80G, 'old_log_disk_size' is still 100G, however, there are two possibility value for
    //           'allowed_new_log_disk_size':
    //           1. the value is 100G because of last shrinking log disk has not been successfully, the log disk record in slog is still 100G
    //              and then we will update log disk size used for palf to 80G, but not update log disk assigned in ObServerLogBlockMGR.
    //              At T3 timestamp, 'new_log_disk_size' is 80G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 80G,
    //              the log disk record in slog is 80G, the log disk assigned in ObServerLogBlockMGR is 80G.
    //           2. the value is 80G because of last shrinking log disk has been successfully, the log disk record in slog is 80G
    //              and then we will update log disk size used for palf to 80G, update log disk assigned in ObServerLogBlockMGR to 80G(assume there is only one tenant).
    //              At T3 timestamp, 'new_log_disk_size' is 25G, 'old_log_disk_size' is 100G, 'allowed_new_log_disk_size' is 25G,
    //              the log disk record in slog is 25G, the log disk assigned in ObServerLogBlockMGR is 25G.
    //
  } else if (OB_FAIL(log_service->update_log_disk_usage_limit_size(new_log_disk_size))) {
    CLOG_LOG(WARN, "failed to update_log_disk_usage_limit_size", K(new_log_disk_size), K(old_log_disk_size),
             K(allowed_new_log_disk_size));
  } else {
    min_log_disk_size_for_all_tenants_ = tmp_log_disk_size;
    CLOG_LOG(INFO, "update_tenant success", KPC(this), K(new_log_disk_size), K(old_log_disk_size),
             K(allowed_new_log_disk_size));
  }

  return ret;
}

int ObServerLogBlockMgr::remove_tenant(const int64_t log_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObServerLogBlockMGR is not inited", K(log_disk_size), KPC(this));
  } else {
    ObSpinLockGuard guard(resize_lock_);
    if (min_log_disk_size_for_all_tenants_ - log_disk_size < 0) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected error, min_log_disk_size_for_all_tenants_ is small than zero",
          K(ret), KPC(this), K(log_disk_size));
    } else {
      min_log_disk_size_for_all_tenants_ -= log_disk_size;
      CLOG_LOG(INFO, "remove tenant from ObServerLogBlockMGR success", KPC(this), K(log_disk_size));
    }
  }
  return ret;
}

int ObServerLogBlockMgr::do_init_(const char *log_pool_base_path)
{
  int ret = OB_SUCCESS;
  bool log_pool_path_exist = false;
  char log_pool_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char log_pool_tmp_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (0 >= ::snprintf(log_pool_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", log_pool_base_path,
                      LOG_POOL_PATH)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "::snprintf failed", K(ret), K(log_pool_base_path));
  } else if (0 >= ::snprintf(log_pool_tmp_path, OB_MAX_FILE_NAME_LENGTH, "%s.tmp",
                             log_pool_path)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "::snprintf failed", K(ret), K(log_pool_base_path));
  } else if (NULL
             == (log_pool_meta_serialize_buf_ = reinterpret_cast<char *>(
                     ob_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_POOL_META_SERIALIZE_SIZE,
                                     "ServerLogPool")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "allocate memory failed", K(ret), KPC(this), K(log_pool_base_path));
  } else if (OB_FAIL(
                 FileDirectoryUtils::is_directory(log_pool_path, log_pool_path_exist))) {
    CLOG_LOG(ERROR, "FileDirectoryUtils::is_exists failed", K(ret),
             K(log_pool_base_path));
  } else if (false == log_pool_path_exist
             && OB_FAIL(prepare_dir_and_create_meta_(log_pool_path, log_pool_tmp_path))) {
    CLOG_LOG(ERROR, "prepare_dir_and_create_meta_ failed", K(ret), K(log_pool_path),
             K(log_pool_tmp_path));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(log_pool_base_path))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), KPC(this), K(log_pool_base_path));
  } else if (-1 == (dir_fd_ = ::open(log_pool_path, OPEN_DIR_FLAG))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::open failed", K(ret), KPC(this), K(errno), K(log_pool_base_path));
  } else if (-1 == (meta_fd_ = ::openat(dir_fd_, "meta", OPEN_FILE_FLAG))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::openat failed", K(ret), KPC(this), K(errno),
             K(log_pool_base_path));
  } else {
    memcpy(log_pool_path_, log_pool_path, OB_MAX_FILE_NAME_LENGTH);
    log_pool_meta_.reset();
    min_block_id_ = 0;
    max_block_id_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObServerLogBlockMgr::prepare_dir_and_create_meta_(const char *log_pool_path,
                                                      const char *log_pool_tmp_path)
{
  int ret = OB_SUCCESS;
  int tmp_dir_fd = -1;
  LogPoolMeta init_log_pool_meta(0, 0, NORMAL_STATUS);
  if (-1 == ::mkdir(log_pool_tmp_path, CREATE_DIR_MODE)) {
    ret = convert_sys_errno();
  } else if (-1 == (tmp_dir_fd = ::open(log_pool_tmp_path, OPEN_DIR_FLAG))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::open failed", K(ret), KPC(this), K(errno), K(log_pool_path),
             K(log_pool_tmp_path));
  } else if (-1
             == (meta_fd_ =
                     ::openat(tmp_dir_fd, "meta", CREATE_FILE_FLAG, CREATE_FILE_MODE))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::openat failed", K(ret), KPC(this), K(errno), K(log_pool_path),
             K(log_pool_tmp_path));
  } else if (OB_FAIL(update_log_pool_meta_guarded_by_lock_(init_log_pool_meta))) {
    CLOG_LOG(ERROR, "update_log_pool_meta_guarded_by_lock_ failed", K(ret),
             K(init_log_pool_meta), K(log_pool_tmp_path), K(log_pool_path));
  } else if (-1 == ::rename(log_pool_tmp_path, log_pool_path)) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::rename failed", K(ret));
  } else {
  }
  if (-1 != meta_fd_) {
    ::close(meta_fd_);
  }
  if (-1 != tmp_dir_fd) {
    ::close(tmp_dir_fd);
  }
  return ret;
}

// step1. scan the directory of 'log_disk_path'(ie: like **/store/clog), get the total
// block count.
//
// step2. scan the directory of 'log_pool_path_'(ie: like **/log_pool), get
// the total block  count, and then trim the directory.
//
// step3. load the meta.
//
// step4. if the 'status_' of meta is EXPANDING_STATUS or SHRINKING_STATUS, continous to
// finish it.
//
// step5. check the total block count of 'log_disk_path' and 'log_pool_path_' whether is
// same as the 'curr_total_size_' of 'log_pool_meta_'.
int ObServerLogBlockMgr::do_load_(const char *log_disk_path)
{
  int ret = OB_SUCCESS;
  int64_t has_allocated_block_cnt = 0;
  ObTimeGuard time_guard("RestartServerBlockMgr", 1 * 1000 * 1000);
  if (OB_FAIL(remove_tmp_file_or_directory_for_tenant_(log_disk_path))) {
    CLOG_LOG(WARN, "remove_tmp_file_or_directory_at failed", K(ret), K(log_disk_path));
  } else if (OB_FAIL(scan_log_disk_dir_(log_disk_path, has_allocated_block_cnt))) {
    CLOG_LOG(WARN, "scan_log_disk_dir_ failed", K(ret), KPC(this), K(log_disk_path),
             K(has_allocated_block_cnt));
  } else if (FALSE_IT(time_guard.click("scan_log_disk_"))
             || OB_FAIL(scan_log_pool_dir_and_do_trim_())) {
    CLOG_LOG(WARN, "scan_log_pool_dir_ failed", K(ret), KPC(this), K(log_disk_path));
  } else if (FALSE_IT(time_guard.click("scan_log_pool_dir_and_do_trim_"))
             || OB_FAIL(load_meta_())) {
    CLOG_LOG(WARN, "load_meta_ failed", K(ret), KPC(this), K(log_disk_path));
  } else if (FALSE_IT(time_guard.click("load_meta_"))
             || OB_FAIL(try_continous_to_resize_(has_allocated_block_cnt * BLOCK_SIZE))) {
    CLOG_LOG(WARN, "try_continous_do_resize_ failed", K(ret), KPC(this),
             K(log_disk_path), K(has_allocated_block_cnt));
  } else if (FALSE_IT(time_guard.click("try_continous_to_resize_"))
             || false
                    == check_log_pool_whehter_is_integrity_(has_allocated_block_cnt
                                                            * BLOCK_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "check_log_pool_whehter_is_integrity_ failed, unexpected error",
             K(ret), KPC(this), K(log_disk_path), K(has_allocated_block_cnt));
  } else {
    block_cnt_in_use_ = has_allocated_block_cnt;
    CLOG_LOG(INFO, "do_load_ success", K(ret), KPC(this), K(time_guard));
  }
  return ret;
}

int ObServerLogBlockMgr::scan_log_disk_dir_(const char *log_disk_path,
                                            int64_t &has_allocated_block_cnt)
{
  return get_has_allocated_blocks_cnt_in_(log_disk_path, has_allocated_block_cnt);
}

int ObServerLogBlockMgr::scan_log_pool_dir_and_do_trim_()
{
  int ret = OB_SUCCESS;
  GetBlockIdListFunctor functor;
  BlockIdArray &block_id_array = functor.block_id_array_;
  // NB: try to clear tmp file or directory in log loop. consider like this:
  //     there may be a expand operation in progress before restarting, if we
  //     not delete tmp directory which used to expand, the new expand operation
  //     will be failed.
  if (OB_FAIL(FileDirectoryUtils::delete_tmp_file_or_directory_at(log_pool_path_))) {
    CLOG_LOG(WARN, "delete_tmp_file_or_directory_at log pool failed", KPC(this));
  } else if (OB_FAIL(palf::scan_dir(log_pool_path_, functor))) {
    CLOG_LOG(ERROR, "scan_dir failed", K(ret), KPC(this));
  } else if (true == block_id_array.empty()) {
    CLOG_LOG(INFO, "the log pool is empty, no need trime", K(ret), KPC(this));
  } else {
    int64_t first_not_continous_block_id_idx = 0;
    auto find_first_not_continous_block_id_func = [&block_id_array,
                                                   &first_not_continous_block_id_idx] {
      // For example, [1, 4, 5, 8, 9],
      // and 'first_not_continous_block_id_idx' is 1
      int64_t count = block_id_array.count();
      std::qsort(&block_id_array[0], count, sizeof(block_id_t),
                 [](const void *x, const void *y) {
                   const block_id_t arg1 = *static_cast<const block_id_t *>(x);
                   const block_id_t arg2 = *static_cast<const block_id_t *>(y);
                   if (arg1 < arg2)
                     return -1;
                   else if (arg1 > arg2)
                     return 1;
                   else
                     return 0;
                 });
      for (int64_t i = 0; i < count - 1; ++i) {
        if (block_id_array[i + 1] - block_id_array[i] == 1) {
          continue;
        } else {
          first_not_continous_block_id_idx = i + 1;
          break;
        }
      }
    };
    find_first_not_continous_block_id_func();
    if (OB_FAIL(trim_log_pool_dir_and_init_block_id_range_(
            block_id_array, first_not_continous_block_id_idx))) {
      CLOG_LOG(ERROR, "trim_log_pool_dir_ failed", K(ret), KPC(this),
               K(first_not_continous_block_id_idx), K(block_id_array));
      // make sure the meta info of both directory has been flushed.
    } else if (OB_FAIL(fsync_after_rename_(dir_fd_))) {
      CLOG_LOG(ERROR, "fsync_after_rename_failed", K(ret), KPC(this));
    } else {
    }
  }
  return ret;
}

int ObServerLogBlockMgr::trim_log_pool_dir_and_init_block_id_range_(
    const BlockIdArray &block_id_array, const int64_t first_need_trim_idx)
{
  int ret = OB_SUCCESS;
  int64_t count = block_id_array.count();
  if (true == block_id_array.empty()) {
    CLOG_LOG(INFO, "the log pool is empty", K(ret), KPC(this), K(first_need_trim_idx));
  } else if (0 == first_need_trim_idx) {
    min_block_id_ = block_id_array[0];
    max_block_id_ = block_id_array[count - 1] + 1;
    CLOG_LOG(INFO, "the log pool is no need trim", K(ret), KPC(this),
             K(first_need_trim_idx), K(block_id_array));
  } else {
    min_block_id_ = block_id_array[0];
    max_block_id_ = block_id_array[first_need_trim_idx - 1] + 1;
    block_id_t dest_block_id = LOG_INVALID_BLOCK_ID;
    char dest_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    char src_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    for (int64_t idx = first_need_trim_idx; idx < count; idx++) {
      dest_block_id = LOG_INVALID_BLOCK_ID;
      memset(dest_block_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      memset(src_block_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (OB_FAIL(get_and_inc_max_block_id_guarded_by_lock_(dest_block_id))) {
        CLOG_LOG(ERROR, "get_and_inc_max_block_id_guarded_by_lock_ failed", K(ret),
                 KPC(this));
      } else if (OB_FAIL(block_id_to_string(dest_block_id, dest_block_path, OB_MAX_FILE_NAME_LENGTH))
          || OB_FAIL(block_id_to_string(block_id_array[idx], src_block_path, OB_MAX_FILE_NAME_LENGTH))) {
        CLOG_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(dest_block_path), K(src_block_path));
      } else if (OB_FAIL(move_block_not_guarded_by_lock_(dir_fd_, dest_block_path, dir_fd_,
                                                         src_block_path))) {
        CLOG_LOG(ERROR, "move_block_not_guarded_by_lock_ failed", K(ret), KPC(this),
                 "src_block_id:", block_id_array[idx]);
      } else {
        CLOG_LOG(INFO, "trim_log_pool_dir_and_init_block_id_range_ success", K(ret),
                 KPC(this), K(block_id_array), K(first_need_trim_idx));
      }
    }
  }
  return ret;
}

int ObServerLogBlockMgr::try_continous_to_resize_(
    const int64_t has_allocated_block_size_byte)
{
  int ret = OB_SUCCESS;
  const int64_t free_size_byte = get_free_size_guarded_by_lock_();
  const int64_t current_total_size_byte = has_allocated_block_size_byte + free_size_byte;
  LogPoolMeta new_log_pool_meta = log_pool_meta_;
  const int64_t old_block_cnt = calc_block_cnt_by_size_(current_total_size_byte);
  const int64_t new_block_cnt =
      calc_block_cnt_by_size_(new_log_pool_meta.next_total_size_);
  const int64_t resize_block_cnt = std::abs(new_block_cnt - old_block_cnt);
  if (NORMAL_STATUS == new_log_pool_meta.status_) {
    CLOG_LOG(INFO, "current status is normal, no need continous do resize", K(ret),
             KPC(this));
  } else if (OB_FAIL(do_resize_(log_pool_meta_, resize_block_cnt, new_log_pool_meta))) {
    CLOG_LOG(INFO, "do_resize_ failed", K(ret), KPC(this), K(new_log_pool_meta));
  } else {
    CLOG_LOG(INFO, "try_continous_do_resize_ success", K(ret), KPC(this),
             K(new_log_pool_meta), K(old_block_cnt), K(new_block_cnt));
  }
  return ret;
}

int ObServerLogBlockMgr::load_meta_()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  LogPoolMetaEntry log_pool_meta_entry;
  if (OB_FAIL(read_unitl_success_(meta_fd_, log_pool_meta_serialize_buf_,
                                  LOG_POOL_META_SERIALIZE_SIZE, 0))) {
    CLOG_LOG(ERROR, "read_unitl_success_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(log_pool_meta_entry.deserialize(
                 log_pool_meta_serialize_buf_, LOG_POOL_META_SERIALIZE_SIZE, pos))) {
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KPC(this), K(pos));
  } else if (false == log_pool_meta_entry.check_integrity()) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "the meta of log pool has corrupted", K(ret), KPC(this));
  } else {
    log_pool_meta_ = log_pool_meta_entry.log_pool_meta_;
    CLOG_LOG(INFO, "load_meta_ success", K(ret), KPC(this));
  }
  return ret;
}

bool ObServerLogBlockMgr::check_log_pool_whehter_is_integrity_(
    const int64_t has_allocated_block_size_byte)
{
  const int64_t free_size_byte = get_free_size_guarded_by_lock_();
  return log_pool_meta_.curr_total_size_
         == has_allocated_block_size_byte + free_size_byte;
}

int ObServerLogBlockMgr::try_resize()
{
  int ret = OB_SUCCESS;
  int64_t log_disk_size = 0;
  int64_t unused_log_disk_percentage = 0;
  int64_t total_log_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObServerLogBlockMgr has not inited", KPC(this));
  } else if (!ATOMIC_LOAD(&is_started_)) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObServerLogBlockMgr not running, can not support resize", KPC(this));
  } else if (OB_FAIL(observer::ObServerUtils::get_log_disk_info_in_config(log_disk_size,
                                                                          unused_log_disk_percentage,
                                                                          total_log_disk_size))) {
    CLOG_LOG(ERROR, "get_log_disk_info_in_config failed", K(log_disk_size), KPC(this));
  } else if (log_disk_size > total_log_disk_size) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    CLOG_LOG(ERROR, "try_resize failed, log disk space is not enough", K(log_disk_size), KPC(this));
  } else if (OB_FAIL(resize_(log_disk_size))) {
    CLOG_LOG(ERROR, "ObServerLogBlockMGR resize failed", K(ret), KPC(this));
  } else {
    CLOG_LOG(INFO, "try_resize success", K(ret), K(log_disk_size), K(total_log_disk_size), KPC(this));
  }
  return ret;
}

bool ObServerLogBlockMgr::check_space_is_enough_(const int64_t log_disk_size) const
{
  bool bool_ret = false;
  int64_t all_tenants_log_disk_size = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tenants_log_disk_size_func_(all_tenants_log_disk_size))) {
    CLOG_LOG(WARN, "get_tenants_log_disk_size_func_ failed", K(ret), K(all_tenants_log_disk_size));
  } else {
    bool_ret = (all_tenants_log_disk_size <= log_disk_size ? true : false);
    CLOG_LOG(INFO, "check_space_is_enough_ finished", K(all_tenants_log_disk_size), K(log_disk_size));
  }
  return bool_ret;
}

int ObServerLogBlockMgr::get_all_tenants_log_disk_size_(int64_t &all_tenants_log_disk_size) const
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  int64_t tenant_count = 0;
  auto func = [&all_tenants_log_disk_size] () -> int{
    int ret = OB_SUCCESS;
    ObLogService *log_service = MTL(ObLogService*);
    PalfOptions opts;
    if (OB_FAIL(log_service->get_palf_options(opts))) {
      CLOG_LOG(WARN, "get_palf_options failed", K(ret), K(all_tenants_log_disk_size));
    } else {
      all_tenants_log_disk_size += opts.disk_options_.log_disk_usage_limit_size_;
    }
    return ret;
  };
  if (OB_FAIL(omt->operate_in_each_tenant(func))) {
    CLOG_LOG(WARN, "operate_in_each_tenant failed", K(ret), K(all_tenants_log_disk_size));
  }
  return ret;
}

int64_t ObServerLogBlockMgr::get_total_size_guarded_by_lock_()
{
  ObSpinLockGuard guard(log_pool_meta_lock_);
  return log_pool_meta_.curr_total_size_;
}

int64_t ObServerLogBlockMgr::get_free_size_guarded_by_lock_()
{
  RLockGuard guard(block_id_range_lock_);
  return BLOCK_SIZE * (max_block_id_ - min_block_id_);
}

int64_t ObServerLogBlockMgr::get_in_use_size_guarded_by_lock_()
{
  RLockGuard guard(block_id_range_lock_);
  return block_cnt_in_use_*BLOCK_SIZE;
}

int ObServerLogBlockMgr::update_log_pool_meta_guarded_by_lock_(const LogPoolMeta &meta)
{
  int ret = OB_SUCCESS;
  {
    ObSpinLockGuard guard(log_pool_meta_lock_);
    log_pool_meta_ = meta;
  }
  LogPoolMetaEntry log_pool_meta_entry(meta);
  log_pool_meta_entry.update_checksum();
  int64_t pos = 0;
  memset(log_pool_meta_serialize_buf_, '\0', LOG_POOL_META_SERIALIZE_SIZE);
  if (OB_FAIL(log_pool_meta_entry.serialize(log_pool_meta_serialize_buf_,
                                            LOG_POOL_META_SERIALIZE_SIZE, pos))) {
    CLOG_LOG(ERROR, "meta serialize failed", K(ret), KPC(this), K(pos));
  } else if (OB_FAIL(write_unitl_success_(meta_fd_, log_pool_meta_serialize_buf_,
                                          LOG_POOL_META_SERIALIZE_SIZE, 0))) {
    CLOG_LOG(ERROR, "write_unitl_success_ failed", K(ret), KPC(this), K(pos));
  } else {
    CLOG_LOG(INFO, "update_log_pool_meta_guarded_by_lock_ success", K(ret), KPC(this));
  }
  return ret;
}

const ObServerLogBlockMgr::LogPoolMeta &
ObServerLogBlockMgr::get_log_pool_meta_guarded_by_lock_() const
{
  ObSpinLockGuard guard(log_pool_meta_lock_);
  return log_pool_meta_;
}

int ObServerLogBlockMgr::get_and_inc_max_block_id_guarded_by_lock_(
    block_id_t &out_block_id,
    const bool remove_block)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(block_id_range_lock_);
  if (OB_UNLIKELY(min_block_id_ > max_block_id_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "min_block_id_ is greater than max_block_id_, unexpected error",
             K(ret), KPC(this));
  } else {
    // max_block_id_ is exclusive range
    out_block_id = max_block_id_++;
    if (remove_block) {
      block_cnt_in_use_--;
    }
    if (block_cnt_in_use_ < 0) {
      CLOG_LOG(WARN, "block_cnt_in_use_ is smaller than zero, unexpected error, may be in process of restart?",
        K(ret), KPC(this), K(block_cnt_in_use_));
    }
  }
  return ret;
}

int ObServerLogBlockMgr::get_and_inc_min_block_id_guarded_by_lock_(
    block_id_t &out_block_id,
    const bool create_block)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(block_id_range_lock_);
  if (OB_UNLIKELY(min_block_id_ == max_block_id_)) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "there is no valid block", K(ret), KPC(this));
  } else if (OB_UNLIKELY(min_block_id_ > max_block_id_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "min_block_id_ is greater than max_block_id_, unexpected error",
             K(ret), KPC(this));
  } else {
    out_block_id = min_block_id_++;
    if (create_block) {
      block_cnt_in_use_++;
    }
  }
  return ret;
}

int ObServerLogBlockMgr::move_block_not_guarded_by_lock_(const FileDesc &dest_dir_fd,
                                                         const char *dest_block_path,
                                                         const FileDesc &src_dir_fd,
                                                         const char *src_block_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(renameat_until_success_(dest_dir_fd, dest_block_path, src_dir_fd, src_block_path))) {
    CLOG_LOG(ERROR, "renameat_until_success_ failed", K(ret), KPC(this), K(dest_dir_fd),
             K(dest_block_path), K(src_dir_fd), K(src_block_path));
  } else {
    CLOG_LOG(TRACE, "move_block_not_guarded_by_lock_ success", K(ret), KPC(this),
             K(dest_dir_fd), K(dest_block_path), K(src_dir_fd), K(src_block_path));
  }
  return ret;
}

int ObServerLogBlockMgr::fsync_after_rename_(const FileDesc &dest_dir_fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fsync_until_success_(dest_dir_fd))) {
    CLOG_LOG(ERROR, "fsync_until_success_ dest failed", K(ret), KPC(this),
             K(dest_dir_fd));
  } else if (dest_dir_fd != dir_fd_ && OB_FAIL(fsync_until_success_(dir_fd_))) {
    CLOG_LOG(ERROR, "fsync_until_success_ src failed", K(ret), KPC(this), K(dest_dir_fd));
  } else {
    ret = OB_SUCCESS;
    CLOG_LOG(INFO, "fsync_after_rename_ success", K(ret), KPC(this), K(dest_dir_fd));
  }
  return ret;
}

int ObServerLogBlockMgr::do_resize_(const LogPoolMeta &old_log_pool_meta,
                                    const int64_t resize_block_cnt,
                                    LogPoolMeta &new_log_pool_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_log_pool_meta_guarded_by_lock_(new_log_pool_meta))) {
    CLOG_LOG(ERROR, "update_log_pool_meta_ failed", K(ret), KPC(this));
  } else if (EXPANDING_STATUS == new_log_pool_meta.status_
             && OB_FAIL(do_expand_(new_log_pool_meta, resize_block_cnt))) {
    CLOG_LOG(ERROR, "do_expand_ failed", K(ret), KPC(this));
  } else if (SHRINKING_STATUS == new_log_pool_meta.status_
             && OB_FAIL(do_shrink_(new_log_pool_meta, resize_block_cnt))) {
    CLOG_LOG(ERROR, "do_shrink_ failed", K(ret), KPC(this));
  } else {
    CLOG_LOG(INFO, "do_expand or do_shrink success", K(ret), KPC(this),
             K(resize_block_cnt));
  }
  int tmp_ret = OB_SUCCESS;
  // If resize success, make 'new_size_byte' valid, othersize, rollback 'log_pool_meta_'
  // to 'origin_log_pool_meta'.
  if (OB_SUCC(ret)) {
    new_log_pool_meta.curr_total_size_ = new_log_pool_meta.next_total_size_;
    new_log_pool_meta.status_ = NORMAL_STATUS;
    if (OB_SUCCESS
        != (tmp_ret = update_log_pool_meta_guarded_by_lock_(new_log_pool_meta))) {
      CLOG_LOG(ERROR, "update_log_pool_meta_ failed", K(ret), KPC(this), K(tmp_ret));
      ret = tmp_ret;
    }
  } else {
    if (OB_SUCCESS
        != (tmp_ret = update_log_pool_meta_guarded_by_lock_(old_log_pool_meta))) {
      CLOG_LOG(ERROR, "update_log_pool_meta_ failed", K(ret), KPC(this), K(tmp_ret));
    }
  }
  return ret;
}

// step1: create new file in tmp_dir, if failed, remove tmp_dir.
// step2: move file in tmp_dir to normal dir, assume move can't be failed.
// step3: remove tmp_dir.
int ObServerLogBlockMgr::do_expand_(const LogPoolMeta &new_log_pool_meta,
                                    const int64_t expand_block_cnt)
{
  int ret = OB_SUCCESS;
  int64_t remain_block_cnt = expand_block_cnt;
  block_id_t dest_start_block_id = 0;
  int resizing_tmp_dir_fd = -1;
  char tmp_dir_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (-1
      == snprintf(tmp_dir_path, OB_MAX_FILE_NAME_LENGTH, "%s/expanding.tmp",
                  log_pool_path_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", K(ret), KPC(this));
  } else if (OB_FAIL(make_resizing_tmp_dir_(tmp_dir_path, resizing_tmp_dir_fd))) {
    CLOG_LOG(ERROR, "make_resizing_tmp_dir_ failed", K(ret), KPC(this),
             K(resizing_tmp_dir_fd));
  } else if (OB_FAIL(allocate_blocks_at_tmp_dir_(resizing_tmp_dir_fd, dest_start_block_id,
                                                 remain_block_cnt))) {
    CLOG_LOG(ERROR, "allocate_blocks_at_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(move_blocks_from_tmp_dir_to_log_pool_(
                 resizing_tmp_dir_fd, dest_start_block_id, expand_block_cnt))) {
    CLOG_LOG(ERROR, "move_blocks_from_tmp_dir_to_log_pool_ failed", K(ret), KPC(this));
    // make sure the meta info of both directory has been flushed.
  } else if (OB_FAIL(fsync_after_rename_(resizing_tmp_dir_fd))) {
    CLOG_LOG(ERROR, "fsync_after_rename_ failed", K(ret), KPC(this),
             K(resizing_tmp_dir_fd));
  } else {
    CLOG_LOG(INFO, "do_expand_ success", K(ret), KPC(this));
  }
  int tmp_ret = OB_SUCCESS;
  if (-1 != resizing_tmp_dir_fd
      && OB_SUCCESS
             != (tmp_ret = remove_resizing_tmp_dir_(tmp_dir_path, resizing_tmp_dir_fd))) {
    CLOG_LOG(ERROR, "resizing_tmp_dir_fd failed", K(ret), KPC(this), K(tmp_dir_path),
             K(resizing_tmp_dir_fd), K(tmp_ret));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObServerLogBlockMgr::make_resizing_tmp_dir_(const char *dir_path,
                                                palf::FileDesc &out_dir_fd)
{
  int ret = OB_SUCCESS;
  if (-1 == (::mkdir(dir_path, CREATE_DIR_MODE))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::mkdir failed", K(ret), KPC(this), K(dir_path));
  } else if (-1 == (out_dir_fd = ::open(dir_path, OPEN_DIR_FLAG))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::open failed", K(ret), KPC(this), K(dir_path));
  } else if (OB_FAIL(fsync_until_success_(dir_fd_))) {
    CLOG_LOG(ERROR, "fsync_until_success_ failed", K(ret), KPC(this), K(dir_path));
  } else {
    CLOG_LOG(INFO, "make_resizing_tmp_dir_ success", K(ret), KPC(this), K(dir_path));
  }
  return ret;
}

int ObServerLogBlockMgr::remove_resizing_tmp_dir_(const char *dir_path,
                                                  const FileDesc &in_fd)
{
  int ret = OB_SUCCESS;
  if (-1 == ::close(in_fd)) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::close failed", K(ret), KPC(this), K(dir_path));
  } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(dir_path))) {
    CLOG_LOG(ERROR, "::rmdir failed", K(ret), KPC(this), K(dir_path));
  } else {
    CLOG_LOG(INFO, "remove_resizing_tmp_dir_ success", K(ret), KPC(this), K(dir_path));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = fsync_until_success_(dir_fd_))) {
    CLOG_LOG(ERROR, "fsync_until_success_ failed", K(ret), K(tmp_ret), KPC(this));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObServerLogBlockMgr::do_shrink_(const LogPoolMeta &new_log_pool_meta,
                                    const int64_t shrink_block_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_blocks_at_log_pool_(shrink_block_cnt))) {
    CLOG_LOG(ERROR, "free_blocks_at_log_pool_ failed", K(ret), KPC(this),
             K(shrink_block_cnt));
  } else {
    CLOG_LOG(INFO, "do_shrink_ success", K(ret), KPC(this), K(shrink_block_cnt));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = fsync_until_success_(dir_fd_))) {
    CLOG_LOG(ERROR, "fsync_until_success_ failed", K(ret), K(tmp_ret), KPC(this), K(dir_fd_));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObServerLogBlockMgr::allocate_blocks_at_tmp_dir_(const FileDesc &dir_fd,
                                                     const block_id_t start_block_id,
                                                     const int64_t block_cnt)
{
  int ret = OB_SUCCESS;
  int64_t remain_block_cnt = block_cnt;
  block_id_t block_id = start_block_id;
  while (OB_SUCC(ret) && remain_block_cnt > 0) {
    if (OB_FAIL(allocate_block_at_tmp_dir_(dir_fd, block_id))) {
      CLOG_LOG(ERROR, "allocate_block_at_tmp_dir_ failed", K(ret), KPC(this), K(dir_fd),
               K(block_id));
    } else {
      remain_block_cnt--;
      block_id++;
    }
  }
  if (-1 == ::fsync(dir_fd)) {
    int tmp_ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::fsync failed", K(ret), K(tmp_ret), KPC(this), K(dir_fd));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObServerLogBlockMgr::allocate_block_at_tmp_dir_(const FileDesc &dir_fd,
                                                    const block_id_t block_id)
{
  int ret = OB_SUCCESS;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  FileDesc fd = -1;
  if (OB_FAIL(block_id_to_string(block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    CLOG_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(dir_fd),
             K(block_id));
  } else if (-1
             == (fd = ::openat(dir_fd, block_path, CREATE_FILE_FLAG, CREATE_FILE_MODE))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::openat failed", K(ret), KPC(this), K(dir_fd), K(block_path));
  } else if (-1 == ::fallocate(fd, 0, 0, BLOCK_SIZE)) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::fallocate failed", K(ret), KPC(this), K(dir_fd), K(block_id),
             K(errno));
  } else {
    CLOG_LOG(INFO, "allocate_block_at_ success", K(ret), KPC(this), K(dir_fd),
             K(block_id));
  }
  if (-1 != fd && -1 == ::close(fd)) {
    int tmp_ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::close failed", K(ret), K(tmp_ret), KPC(this), K(dir_fd), K(block_path));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObServerLogBlockMgr::free_blocks_at_log_pool_(const int64_t block_cnt)
{
  int ret = OB_SUCCESS;
  int64_t remain_block_cnt = block_cnt;
  block_id_t block_id = LOG_INVALID_BLOCK_ID;
  while (OB_SUCC(ret) && remain_block_cnt > 0) {
    if (OB_FAIL(get_and_inc_min_block_id_guarded_by_lock_(block_id))) {
      CLOG_LOG(ERROR,
               "get_and_inc_min_block_id_guarded_by_lock_ failed, unexpected error",
               K(ret), KPC(this), K(remain_block_cnt), K(block_cnt));
    } else if (OB_FAIL(free_block_at_(dir_fd_, block_id))) {
      CLOG_LOG(ERROR, "free_block_at_ failed", K(ret), KPC(this), K(block_id));
    } else {
      remain_block_cnt--;
    }
  }
  return ret;
}

int ObServerLogBlockMgr::free_block_at_(const FileDesc &src_dir_fd,
                                        const block_id_t src_block_id)
{
  int ret = OB_SUCCESS;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  FileDesc fd = -1;
  if (OB_FAIL(block_id_to_string(src_block_id, block_path, OB_MAX_FILE_NAME_LENGTH))) {
    CLOG_LOG(ERROR, "src_block_id_to_string failed", K(ret), KPC(this), K(src_dir_fd),
             K(src_block_id));
  } else if (OB_FAIL(unlinkat_until_success_(src_dir_fd, block_path, 0))) {
    CLOG_LOG(ERROR, "unlinkat_until_success_i failed", K(ret), KPC(this), K(src_dir_fd),
             K(src_block_id));
  } else {
    CLOG_LOG(INFO, "free_block_at_ success", K(ret), KPC(this), K(src_dir_fd),
             K(src_block_id));
  }
  return ret;
}

int ObServerLogBlockMgr::move_blocks_from_tmp_dir_to_log_pool_(
    const FileDesc &src_dir_fd, const block_id_t start_block_id, const int64_t block_cnt)
{
  int ret = OB_SUCCESS;
  block_id_t dest_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t src_block_id = start_block_id;
  block_id_t remain_block_cnt = block_cnt;
  char dest_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char src_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  while (OB_SUCC(ret) && remain_block_cnt > 0) {
    memset(dest_block_path, '\0', OB_MAX_FILE_NAME_LENGTH);
    memset(src_block_path, '\0', OB_MAX_FILE_NAME_LENGTH);
    if (OB_FAIL(get_and_inc_max_block_id_guarded_by_lock_(dest_block_id))) {
      CLOG_LOG(ERROR, "get_and_inc_max_block_id_guarded_by_lock_ failed", K(ret),
               KPC(this), K(src_dir_fd), K(src_block_id));
    } else if (OB_FAIL(block_id_to_string(dest_block_id, dest_block_path, OB_MAX_FILE_NAME_LENGTH))
        || OB_FAIL(block_id_to_string(src_block_id, src_block_path, OB_MAX_FILE_NAME_LENGTH))) {
      CLOG_LOG(ERROR, "block_id_to_string failed", K(ret), KPC(this), K(dest_block_id), K(src_block_id));
    } else if (OB_FAIL(move_block_not_guarded_by_lock_(dir_fd_, dest_block_path, src_dir_fd,
                                                       src_block_path))) {
      CLOG_LOG(ERROR, "move_block_not_guarded_by_lock_ failed", K(ret), KPC(this),
               K(src_dir_fd), K(src_block_id));
    } else {
      remain_block_cnt--;
      src_block_id++;
    }
  }
  return ret;
}

int64_t ObServerLogBlockMgr::calc_block_cnt_by_size_(const int64_t curr_total_size)
{
  const int64_t resize_bytes = lower_align_(curr_total_size);
  const int64_t blocks = resize_bytes / BLOCK_SIZE;
  return blocks;
}

int ObServerLogBlockMgr::get_has_allocated_blocks_cnt_in_(
    const char *log_disk_path, int64_t &has_allocated_block_cnt)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  std::regex pattern_log(".*/tenant_[1-9]\\d*/[1-9]\\d*/log");
  std::regex pattern_meta(".*/tenant_[1-9]\\d*/[1-9]\\d*/meta");
  if (NULL == (dir = opendir(log_disk_path))) {
    ret = OB_ERR_SYS;
    CLOG_LOG(WARN, "opendir failed", K(log_disk_path));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",
                               log_disk_path, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(log_disk_path),
                K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        CLOG_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
      } else if (false == is_dir) {
      } else if (true == std::regex_match(current_file_path, pattern_log)
                 || true == std::regex_match(current_file_path, pattern_meta)) {
        GetBlockCountFunctor functor(current_file_path);
        if (OB_FAIL(palf::scan_dir(current_file_path, functor))) {
          LOG_DBA_ERROR(OB_ERR_UNEXPECTED, "Attention!!!", "There are several files in the log directory that are not generated by "
                        "OceanBase. Please confirm whether manual deletion is required",
                        "log directory", current_file_path);
        } else {
          has_allocated_block_cnt += functor.get_block_count();
          CLOG_LOG(INFO, "get_has_allocated_blocks_cnt_in_ success", K(ret),
                   K(current_file_path), "block_cnt", functor.get_block_count());
        }
      } else if (OB_FAIL(get_has_allocated_blocks_cnt_in_(current_file_path,
                                                          has_allocated_block_cnt))) {
        CLOG_LOG(WARN, "get_has_allocated_blocks_cnt_in_ failed", K(ret),
                 K(current_file_path), K(has_allocated_block_cnt));
      } else {
      }
    }
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int64_t ObServerLogBlockMgr::lower_align_(const int64_t new_size_byte)
{
  return lower_align(new_size_byte, BLOCK_SIZE);
}

int ObServerLogBlockMgr::remove_tmp_file_or_directory_for_tenant_(const char *log_disk_path)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  std::regex pattern_tenant(".*/tenant_[1-9]\\d*");
  struct dirent *entry = NULL;
  if (NULL == (dir = opendir(log_disk_path))) {
    ret = OB_ERR_SYS;
    CLOG_LOG(WARN, "opendir failed", K(log_disk_path));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",
                               log_disk_path, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(log_disk_path),
                K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        CLOG_LOG(WARN, "is_directory failed", K(ret), K(entry->d_name));
      } else if (false == is_dir) {
        CLOG_LOG(ERROR, "is not diectory, unexpected", K(ret), K(log_disk_path), K(current_file_path));
      } else if (true == std::regex_match(current_file_path, pattern_tenant)) {
        if (OB_FAIL(remove_tmp_file_or_directory_at(current_file_path, this))) {
          CLOG_LOG(ERROR, "this dir is tenant, remove_tmp_file_or_directory_at failed", K(ret), K(current_file_path));
        } else {
          CLOG_LOG(INFO, "this dir is tenant, remove_tmp_file_or_directory_at success", K(ret), K(current_file_path));
        }
      } else {
      }
    }
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int ObServerLogBlockMgr::fallocate_until_success_(const palf::FileDesc &src_fd,
                                                  const int64_t block_size)
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == ::fallocate(src_fd, 0, 0, block_size)) {
      ret = convert_sys_errno();
      CLOG_LOG(ERROR, "::fallocate failed", K(ret), KPC(this), K(src_fd), K(block_size));
      ob_usleep(SLEEP_TS_US);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int ObServerLogBlockMgr::unlinkat_until_success_(const palf::FileDesc &src_dir_fd,
                                                 const char *block_path, const int flag)
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == ::unlinkat(src_dir_fd, block_path, flag)) {
      ret = convert_sys_errno();
      CLOG_LOG(ERROR, "::unlink failed", K(ret), KPC(this), K(src_dir_fd), K(block_path),
               K(flag));
      ob_usleep(SLEEP_TS_US);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int ObServerLogBlockMgr::fsync_until_success_(const FileDesc &dest_dir_fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fsync_with_retry(dest_dir_fd))) {
    CLOG_LOG(ERROR, "fsync_with_retry failed", KR(ret), KPC(this), K(dest_dir_fd));
  }
  return ret;
}

int ObServerLogBlockMgr::renameat_until_success_(const FileDesc &dest_dir_fd,
                                                 const char *dest_block_path,
                                                 const FileDesc &src_dir_fd,
                                                 const char *src_block_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(renameat_with_retry(src_dir_fd, src_block_path, dest_dir_fd, dest_block_path))) {
    CLOG_LOG(ERROR, "renameat_with_retry failed", K(ret), KPC(this), K(dest_dir_fd),
             K(dest_block_path), K(src_dir_fd), K(src_block_path));
  }
  return ret;
}

int ObServerLogBlockMgr::write_unitl_success_(const FileDesc &dest_fd,
                                              const char *src_buf,
                                              const int64_t src_buf_len,
                                              const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    if (src_buf_len != (write_size = ob_pwrite(dest_fd, src_buf, src_buf_len, offset))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        ret = convert_sys_errno();
        CLOG_LOG(ERROR, "ob_pwrite failed", K(ret), KPC(this), K(offset), K(write_size));
      }
      ob_usleep(SLEEP_TS_US);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

int ObServerLogBlockMgr::read_unitl_success_(const FileDesc &src_fd, char *dest_buf,
                                             const int64_t dest_buf_len,
                                             const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    if (dest_buf_len != (read_size = ob_pread(src_fd, dest_buf, dest_buf_len, offset))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        ret = convert_sys_errno();
        CLOG_LOG(ERROR, "ob_pread failed", K(ret), KPC(this), K(offset), K(read_size));
      }
      ob_usleep(SLEEP_TS_US);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  return ret;
}

void ObServerLogBlockMgr::LogPoolMeta::reset()
{
  curr_total_size_ = 0;
  next_total_size_ = 0;
  status_ = NORMAL_STATUS;
}

bool ObServerLogBlockMgr::LogPoolMeta::resizing() const
{
  return EXPANDING_STATUS == status_ || SHRINKING_STATUS == status_;
}

DEFINE_SERIALIZE(ObServerLogBlockMgr::LogPoolMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, curr_total_size_))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, next_total_size_))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, status_))) {
    CLOG_LOG(ERROR, "serialize failed", K(ret), KPC(this), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObServerLogBlockMgr::LogPoolMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &curr_total_size_))
      || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &next_total_size_))
      || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &status_))) {
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KPC(this), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    CLOG_LOG(INFO, "deserialize LogPoolMeta success", KPC(this), K(buf));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObServerLogBlockMgr::LogPoolMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(sizeof(curr_total_size_));
  size += serialization::encoded_length_i64(sizeof(next_total_size_));
  size += serialization::encoded_length_i64(sizeof(status_));
  return size;
}

int ObServerLogBlockMgr::GetBlockIdListFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else {
    bool is_number = true;
    const char *entry_name = entry->d_name;
    for (int64_t i = 0; is_number && i < sizeof(entry->d_name); ++i) {
      if ('\0' == entry_name[i]) {
        break;
      } else if (!isdigit(entry_name[i])) {
        is_number = false;
      }
    }
    if (!is_number) {
      // do nothing, skip invalid block like tmp
    } else {
      block_id_t block_id = static_cast<block_id_t>(strtol(entry->d_name, nullptr, 10));
      if (OB_FAIL(block_id_array_.push_back(block_id))) {
        CLOG_LOG(ERROR, "push_back failed", K(ret), K(block_id), K(block_id_array_));
      }
    }
  }
  return ret;
}

void ObServerLogBlockMgr::LogPoolMetaEntry::update_checksum()
{
  checksum_ = calc_checksum_();
  CLOG_LOG(INFO, "update_checksum success", KPC(this));
}

int64_t ObServerLogBlockMgr::LogPoolMetaEntry::calc_checksum_()
{
  int64_t checksum = 0;
  int64_t header_checksum_len = sizeof(*this) - sizeof(checksum_);
  checksum = static_cast<int64_t>(ob_crc64(this, header_checksum_len));
  return checksum;
}

bool ObServerLogBlockMgr::LogPoolMetaEntry::check_integrity()
{
  return checksum_ == calc_checksum_();
}

DEFINE_SERIALIZE(ObServerLogBlockMgr::LogPoolMetaEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))
      || OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))
      || OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, flag_))
      || OB_FAIL(log_pool_meta_.serialize(buf, buf_len, new_pos))
      || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, checksum_))) {
    CLOG_LOG(ERROR, "serialize failed", K(ret), KPC(this), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObServerLogBlockMgr::LogPoolMetaEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_))
      || OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))
      || OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &flag_))
      || OB_FAIL(log_pool_meta_.deserialize(buf, data_len, new_pos))
      || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &checksum_))) {
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KPC(this), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    CLOG_LOG(INFO, "deserialize LogPoolMeta success", KPC(this), K(buf));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObServerLogBlockMgr::LogPoolMetaEntry)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(sizeof(magic_));
  size += serialization::encoded_length_i16(sizeof(version_));
  size += serialization::encoded_length_i32(sizeof(flag_));
  size += log_pool_meta_.get_serialize_size();
  size += serialization::encoded_length_i64(sizeof(checksum_));
  return size;
}
} // namespace logservice
} // namespace oceanbase
