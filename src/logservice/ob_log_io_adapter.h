/** Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_IO_ADAPTER_
#define OCEANBASE_LOGSERVICE_OB_LOG_IO_ADAPTER_

#include "common/storage/ob_io_device.h"                      // ObIODevice
#include "lib/function/ob_function.h"                         // ObFunction
#include "lib/hash/ob_hashmap.h"                              // ObHashMap
#include "lib/lock/ob_tc_rwlock.h"                            // TCRWLock
#include "lib/lock/ob_spin_lock.h"                            // ObSpinLock
#include "lib/utility/ob_print_utils.h"                       // TO_STRING_KV
#include "lib/task/ob_timer.h"                                // ObTimerTask
namespace oceanbase
{
namespace common
{
class ObIOManager;
class ObDeviceManager;
class ObBaseDirEntryOperator;
struct ObIOInfo;
}
namespace share
{
class ObLocalDevice;
class ObResourceManager;
}
namespace logservice
{
class ObLogDevice;
enum class ObLogIOMode {
  INVALID = 0,
  LOCAL = 1,
  REMOTE = 2
};

bool is_valid_log_io_mode(const ObLogIOMode mode);

inline const char *log_io_mode_user_str(const ObLogIOMode mode)
{
  #define LOG_IO_MODE_STR(x) case(ObLogIOMode::x): return #x
  switch (mode)
  {
    LOG_IO_MODE_STR(LOCAL);
    LOG_IO_MODE_STR(REMOTE);
    default:
      return "InvalidMode";
  }
  #undef LOG_IO_MODE_STR
}

class ObLogIOInfo {
public:
  ObLogIOInfo();
  ~ObLogIOInfo();
  int init(const ObLogIOMode &io_mode,
           const char *log_store_addr,
           const int64_t cluster_id);
  bool is_valid() const;
  void reset();
  const ObLogIOMode &get_log_io_mode() const;
  const int64_t &get_cluster_id() const;
  const ObAddr &get_addr() const;
  TO_STRING_KV("io_mode", log_io_mode_user_str(io_mode_), K_(cluster_id), K_(log_store_addr));
private:
  ObLogIOMode io_mode_;
  ObAddr log_store_addr_;
  int64_t cluster_id_;
};

struct SwitchLogIOModeCbKey {
  SwitchLogIOModeCbKey();
  SwitchLogIOModeCbKey(const char *key);
  ~SwitchLogIOModeCbKey();
  char value_[OB_MAX_FILE_NAME_LENGTH];
  uint64_t hash() const;
  int assign(const SwitchLogIOModeCbKey &key);
  int hash(uint64_t &val) const;
  TO_STRING_KV(K_(value), K(hash()));
};

bool operator==(const SwitchLogIOModeCbKey &lhs, const SwitchLogIOModeCbKey &rhs);

typedef ObFunction<int(ObIODevice *prev_io_device, ObIODevice *io_device, const int64_t align_size)> SwitchLogIOModeCb;

class ObLogIOAdapter
{
public:
  static ObLogIOAdapter &get_instance() {
    static ObLogIOAdapter instance;
    return instance;
  }
public:
  ObLogIOAdapter() : using_mode_(ObLogIOMode::INVALID),
                     using_device_(NULL),
                     log_local_device_(NULL),
                     log_remote_device_(NULL),
                     io_manager_(NULL),
                     device_manager_(NULL),
                     clog_dir_(),
                     disk_io_thread_count_(-1),
                     max_io_depth_(-1),
                     flying_fd_count_(0),
                     is_inited_(false) {}
  int init(const char *clog_dir,
           const int64_t disk_io_thread_count,
           const int64_t max_io_depth,
           const ObLogIOInfo &log_io_info,
           common::ObIOManager *io_manager,
           common::ObDeviceManager *device_manager);
  void destroy();
  int switch_log_io_mode(const ObLogIOInfo &io_info);
  share::ObLocalDevice* get_local_device();
public:
  int pread(const int64_t in_read_size,
            ObIOInfo &info,
            int64_t &read_size,
            ObIOManager *io_manager);
  int pwrite(const int64_t in_write_size,
             ObIOInfo &info,
             int64_t &write_size,
             ObIOManager *io_manager);
  int open(const char *block_path,
           const int flags,
           const mode_t mode,
           ObIOFd &io_fd);
  int close(ObIOFd &io_fd);
  int pread(const ObIOFd &io_fd,
            const int64_t count,
            const int64_t offset,
            char *buf,
            int64_t &out_read_size);
  int pwrite(const ObIOFd &io_fd,
             const char *buf,
             const int64_t count,
             const int64_t offset,
             int64_t &write_size);
  int truncate(const ObIOFd &fd, const int64_t offset);
  int fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len);
  int fsync(const ObIOFd &fd);
  int fsync_dir(const char *dir_name);
  int stat(const char *pathname,
           common::ObIODFileStat &statbuf);
  int fstat(const common::ObIOFd &fd,
            common::ObIODFileStat &statbuf);
  int rename(const char *oldpath,
             const char *newpath);
  int unlink(const char *pathname);

  // dir interface
  int scan_dir(const char *dir_name,
               common::ObBaseDirEntryOperator &op);
  int mkdir(const char *pathname,
            mode_t mode);
  int rmdir(const char *pathname);

  int register_cb(const SwitchLogIOModeCbKey &key, SwitchLogIOModeCb &cb);
  void unregister_cb(const SwitchLogIOModeCbKey &key);
  int try_switch_log_io_mode();

  // batch interface
  int batch_fallocate(const char *dir_name,
                      const int64_t block_count,
                      const int64_t block_size);
  int64_t choose_align_size() const;
  TO_STRING_KV(K_(using_mode), KP(using_device_), K_(flying_fd_count), KP(log_local_device_), KP(log_remote_device_));
  // TODO: reconfig?
private:
  int init_io_device_(const char *clog_dir,
                      const ObLogIOInfo &io_info,
                      const int64_t disk_io_thread_count,
                      const int64_t max_io_depth,
                      ObDeviceManager *device_manager,
                      ObIOManager *io_manager);
  int init_local_device_(const char *clog_dir,
                         const int64_t disk_io_thread_count,
                         const int64_t max_io_depth,
                         ObDeviceManager *device_manager,
                         ObIOManager *io_manager);
  int init_remote_device_(const char *clog_dir,
                          const int64_t disk_io_thread_count,
                          const int64_t max_io_depth,
                          const ObAddr &addr,
                          const int64_t cluster_idx,
                          ObDeviceManager *device_manager,
                          ObIOManager *io_manager);
  int switch_log_io_mode_to_local_();
  int switch_log_io_mode_to_remote_(const ObLogIOInfo &io_info);
  int execute_switch_log_io_mode_cb_(ObIODevice *io_device,
                                     const ObLogIOMode &io_mode);
  void wait_flying_fd_count_to_zero_();
  int deal_with_state_not_match_();

  const ObLogIOMode &get_using_mode_();

  int batch_fallocate_for_local_(const char *dir_name,
                                 const int64_t block_count,
                                 const int64_t block_size);
  int batch_fallocate_for_remote_(const char *dir_name,
                                  const int64_t block_count,
                                  const int64_t block_size);
  int64_t choose_align_size_(const ObLogIOMode &io_mode) const;
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  static const int64_t BUCKET_NUM = 16;
private:
  struct ExecuteCbFunctor {
    ExecuteCbFunctor(ObIODevice *prev_io_device,
                     ObIODevice *io_device,
                     int64_t align_size);
    ~ExecuteCbFunctor();
    int operator()(common::hash::HashMapPair<SwitchLogIOModeCbKey, SwitchLogIOModeCb> &pair);
    ObIODevice *prev_io_device_;
    ObIODevice *io_device_;
    int64_t align_size_;
  };
private:
  ObLogIOMode using_mode_;
  ObIODevice *using_device_;
  share::ObLocalDevice *log_local_device_;
  ObLogDevice *log_remote_device_;
  ObIOManager *io_manager_;
  common::ObDeviceManager *device_manager_;
  char clog_dir_[OB_MAX_FILE_NAME_LENGTH];
  int disk_io_thread_count_;
  int max_io_depth_;
  int64_t flying_fd_count_;
  mutable TCRWLock using_device_lock_;
  common::hash::ObHashMap<SwitchLogIOModeCbKey, SwitchLogIOModeCb> cb_map_;

  ObSpinLock cb_map_lock_;
  bool is_inited_;
};

#define LOG_IO_ADAPTER ::oceanbase::logservice::ObLogIOAdapter::get_instance()
} // namespace logservice
} // namespace oceanbase

#endif
