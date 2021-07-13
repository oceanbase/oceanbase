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

#ifndef OB_LOG_DISK_MANAGER_H_
#define OB_LOG_DISK_MANAGER_H_

#include <dirent.h>
#include "clog/ob_log_define.h"
#include "clog/ob_log_file_pool.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/task/ob_timer.h"

#define OB_CLOG_DISK_MGR (oceanbase::common::ObCLogDiskManager::get_instance())
#define OB_ILOG_DISK_MGR (oceanbase::common::ObILogDiskManager::get_instance())
#define OB_SLOG_DISK_MGR (oceanbase::common::ObSLogDiskManager::get_instance())

namespace oceanbase {
namespace common {
enum ObRedoLogType {
  OB_REDO_TYPE_INVALID = 0,
  OB_REDO_TYPE_CLOG = 1,
  OB_REDO_TYPE_ILOG = 2,
  OB_REDO_TYPE_SLOG = 3,
};

enum ObLogDiskState {
  OB_LDS_INVALID = 0,

  // when a new disk is added
  OB_LDS_NEW = 1,

  // when a NEW disk is detected by monitor task, it changed to RESTORE and start copying files
  OB_LDS_RESTORE = 2,

  // when all content including the last file is complete, disk changes to GOOD state.
  // A GOOD disk is enabled for both read and write now
  OB_LDS_GOOD = 3,

  OB_LDS_BAD = 4
};

struct ObLogFdInfo {
public:
  int fd_;
  int64_t disk_id_;
  int64_t file_id_;
  int64_t refresh_time_;

  ObLogFdInfo() : fd_(-1), disk_id_(-1), file_id_(-1), refresh_time_(0)
  {}
  OB_INLINE void reset()
  {
    fd_ = -1;
    disk_id_ = -1;
    file_id_ = -1;
    refresh_time_ = 0;
  }

  bool is_valid() const;

  TO_STRING_KV(K_(fd), K_(disk_id), K_(file_id), K_(refresh_time));
};

class ObLogDiskInfo {
public:
  ObLogDiskInfo()
      : disk_id_(-1),
        state_(OB_LDS_INVALID),
        state_modify_timestamp_(0),
        restore_start_file_id_(-1),
        restore_start_offset_(-1),
        log_dir_(),
        file_pool_(),
        is_inited_(false)
  {
    dir_path_[0] = '\0';
  }

  ~ObLogDiskInfo()
  {
    disk_id_ = -1;
    reuse();
  }

  int init(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);
  void reuse();

  OB_INLINE int64_t get_disk_id() const
  {
    return disk_id_;
  }
  OB_INLINE void set_disk_id(const int64_t disk_id)
  {
    disk_id_ = disk_id;
  }
  OB_INLINE const char* get_disk_path() const
  {
    return dir_path_;
  }
  OB_INLINE ObLogDiskState get_state() const
  {
    return (ObLogDiskState)ATOMIC_LOAD(&state_);
  }
  OB_INLINE void set_state(ObLogDiskState new_state)
  {
    ATOMIC_STORE(&state_, new_state);
    ATOMIC_STORE(&state_modify_timestamp_, ObTimeUtility::current_time());
  }
  OB_INLINE void set_state(ObLogDiskState old_state, ObLogDiskState new_state)
  {
    if (old_state == ATOMIC_CAS(&state_, old_state, new_state)) {
      ATOMIC_STORE(&state_modify_timestamp_, ObTimeUtility::current_time());
    }
  }
  OB_INLINE int64_t get_state_timestamp() const
  {
    return ATOMIC_LOAD(&state_modify_timestamp_);
  }
  OB_INLINE clog::ObLogWriteFilePool* get_file_pool()
  {
    return &file_pool_;
  }
  OB_INLINE const clog::ObLogWriteFilePool* get_file_pool() const
  {
    return &file_pool_;
  }
  OB_INLINE int64_t get_restore_start_file_id() const
  {
    return ATOMIC_LOAD(&restore_start_file_id_);
  }
  OB_INLINE int64_t get_restore_start_offset() const
  {
    return ATOMIC_LOAD(&restore_start_offset_);
  }
  // @brief: log restore start file_id and offset. It is used when catching up the last
  //         writing log file, ObLogFileDescriptor uses this method to inform ObLogDiskManager
  void restore_start(const int64_t file_id, const int64_t offset);

  TO_STRING_KV(K_(disk_id), K_(dir_path), K_(state), K_(state_modify_timestamp), K_(restore_start_file_id),
      K_(restore_start_offset), K_(log_dir), K_(file_pool), K_(is_inited));

private:
  OB_INLINE void set_disk_path(const char* path)
  {
    if (STRLEN(path) >= OB_MAX_FILE_NAME_LENGTH) {
      STRNCPY(dir_path_, path, OB_MAX_FILE_NAME_LENGTH - 1);
      dir_path_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
    } else {
      STRNCPY(dir_path_, path, OB_MAX_FILE_NAME_LENGTH - 1);
    }
  }

private:
  int64_t disk_id_;
  char dir_path_[OB_MAX_FILE_NAME_LENGTH];
  int64_t state_;
  int64_t state_modify_timestamp_;
  int64_t restore_start_file_id_;
  int64_t restore_start_offset_;
  clog::ObLogDir log_dir_;
  clog::ObLogWriteFilePool file_pool_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObLogDiskInfo);
};

class ObLogDiskManager {
public:
  class BaseDiskIterator {
  public:
    BaseDiskIterator(const ObLogDiskManager* disk_mgr, int64_t idx)
    {
      disk_mgr_ = disk_mgr;
      cur_idx_ = idx;
    }
    virtual ~BaseDiskIterator()
    {
      disk_mgr_ = NULL;
      cur_idx_ = -1;
    }

    OB_INLINE const ObLogDiskInfo* operator->() const
    {
      const ObLogDiskInfo* ret_disk = NULL;
      if (cur_idx_ < 0 || cur_idx_ >= ObLogDiskManager::MAX_DISK_COUNT) {
        // do nothing
      } else {
        ret_disk = &(disk_mgr_->disk_slots_[cur_idx_]);
      }
      return ret_disk;
    }

    OB_INLINE BaseDiskIterator& operator++()
    {
      int64_t i = cur_idx_ + 1;
      for (; i < ObLogDiskManager::MAX_DISK_COUNT; i++) {
        if (is_qualified_state(disk_mgr_->disk_slots_[i].get_state())) {
          break;
        }
      }
      cur_idx_ = i;
      return *this;
    }

    OB_INLINE bool operator==(const BaseDiskIterator& other) const
    {
      return (other.disk_mgr_ == disk_mgr_) && (other.cur_idx_ == cur_idx_);
    }

    OB_INLINE bool operator!=(const BaseDiskIterator& other) const
    {
      return (other.disk_mgr_ != disk_mgr_) || (other.cur_idx_ != cur_idx_);
    }

  protected:
    virtual bool is_qualified_state(const ObLogDiskState state)
    {
      UNUSED(state);
      return true;
    }

  private:
    const ObLogDiskManager* disk_mgr_;
    int64_t cur_idx_;
  };

  class ReadWriteDiskIterator : public BaseDiskIterator {
  public:
    ReadWriteDiskIterator(const ObLogDiskManager* disk_mgr, int64_t idx) : BaseDiskIterator(disk_mgr, idx){};
    virtual ~ReadWriteDiskIterator(){};

  protected:
    virtual bool is_qualified_state(const ObLogDiskState state)
    {
      return OB_LDS_GOOD == state || OB_LDS_RESTORE == state;
    }
  };

  // Persistent information of log restore progress, it is serialized in LOG_RESTORE file.
  // Log files are classified into two groups:
  // COPY_GROUP: except the last n log files, all their previous log files are in this group.
  // CATCHUP_GROUP: include the last n log files. N is decided by when write API set the
  //                restore start file id and offset
  // For example, log file 1, 2, ..., 100. When restore start, file 100 is being written.
  // COPY_GROUP includes file 1 to 99 and CATCH_GROUP includes 100.
  // The restore progress is checkpoint and persisted by ObLogDiskManager::write_restore_progress
  // method. This method is called when:
  // 1) Prepare COPY_GROUP
  // 2) Each time a file copy finished
  // 3) All COPY_GROUP finish
  // 4) Get restore start file and offset from disk info
  // 5) Catch up finish
  class LogRestoreProgress {
  public:
    LogRestoreProgress()
    {
      reset();
    }
    ~LogRestoreProgress(){};

    // Latter data member should be 64 bits ALIGNED
    // && NO virtual function in this class
    // && checksum_ is the FIRST member
    int64_t checksum_;
    int16_t length_;
    int16_t version_;
    int16_t catchup_complete_;    // flag indicate if catch up finish
    int16_t copy_complete_;       // flag indicate if copy finish
    int64_t catchup_file_id_;     // the writing log file when restore start
    int64_t catchup_offset_;      // the offset of the writing log file
    int64_t copy_start_file_id_;  // the upper boundary of coping log files
    int64_t copied_file_id_;      // the minimum successfully copied log file id

    TO_STRING_KV(K_(checksum), K_(length), K_(version), K_(catchup_complete), K_(copy_complete), K_(catchup_file_id),
        K_(catchup_offset), K_(copy_start_file_id), K_(copied_file_id));

    static const int16_t DATA_VERSION = 1;

  public:
    OB_INLINE bool is_catchup_start() const
    {
      return catchup_file_id_ > 0 && catchup_offset_ >= 0;
    }
    OB_INLINE bool is_catchup_complete() const
    {
      return catchup_complete_ > 0;
    }
    OB_INLINE bool is_copy_start() const
    {
      return copy_start_file_id_ > 0;
    }
    OB_INLINE bool is_copy_complete() const
    {
      return copy_complete_ > 0;
    }

    void reset();
    int serialize(char* buf, const int64_t buf_len, int64_t& pos);
    int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    int64_t get_serialize_size(void) const;

  private:
    int check();
  };

public:
  ObLogDiskManager();
  virtual ~ObLogDiskManager();

  virtual int init(const char* log_dir, const int64_t file_size);
  int init(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);
  // @brief This method sync system file descriptor based on the given file id and disk id
  //        It is designed for high performance lock free and only memory check.
  // @param [in] file_id: id of the log file
  // @param [in] disk_id: target disk id
  // @param [in] is_tmp: whether the log file is named by .tmp suffix
  // @param [in] enable_write: open the system fd with write flag
  // @param [in] offset: works when enable write, tell the disk manager write start offset
  // @param [out] fd_info: the up-to-date system fd and related information
  // @return OB_SUCCESS:: succeed sync fd
  //         other: error happened
  int sync_system_fd(const int64_t file_id, const int64_t disk_id, const bool is_tmp, const bool enable_write,
      const int64_t offset, ObLogFdInfo& fd_info);
  void destroy();
  int set_bad_disk(const int64_t disk_id);

  void update_min_using_file_id(const uint32_t file_id);
  void update_min_file_id(const uint32_t file_id);
  void update_max_file_id(const uint32_t file_id);
  void try_recycle_file();
  int update_free_quota();

  uint32_t get_min_using_file_id();
  uint32_t get_min_file_id();
  int64_t get_free_quota() const;
  bool is_disk_space_enough() const;
  bool free_quota_warn();
  int get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id);
  int get_total_used_size(int64_t& total_size);
  int exist(const int64_t file_id, bool& is_exist);
  int fstat(const int64_t file_id, struct stat* file_stat);
  const char* get_dir_name();
  int get_total_disk_space(int64_t& total_space) const;

  BaseDiskIterator begin_all() const;
  BaseDiskIterator end_all() const;
  ReadWriteDiskIterator begin() const;
  ReadWriteDiskIterator end() const;

  static ObLogDiskManager* get_disk_manager(const ObRedoLogType log_type);
  static const int64_t MAX_DISK_COUNT = 5;

private:
  struct DirScanResult {
    int64_t disk_id_;
    int64_t min_log_id_;
    int64_t max_log_id_;
    bool continuous_;
    bool has_log_;
    bool is_restoring_;

    DirScanResult()
        : disk_id_(-1), min_log_id_(-1), max_log_id_(-1), continuous_(false), has_log_(false), is_restoring_(false)
    {}

    TO_STRING_KV(K_(disk_id), K_(min_log_id), K_(max_log_id), K_(continuous), K_(has_log), K_(is_restoring));
  };

  class MonitorTask : public common::ObTimerTask {
  public:
    MonitorTask() : disk_mgr_(NULL)
    {}
    virtual ~MonitorTask()
    {
      destroy();
    }
    int init(ObLogDiskManager* disk_mgr);
    void destroy()
    {
      disk_mgr_ = NULL;
    }
    virtual void runTimerTask();

  private:
    ObLogDiskManager* disk_mgr_;
    DISALLOW_COPY_AND_ASSIGN(MonitorTask);
  };

  int detect_disks(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);
  int single_disk_check(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);
  int add_disk(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type);
  // @brief A valid disk name for multiple disks must satisfy:
  //        - doesn't have ".bad" suffix
  //        - have "disk" prefix
  //        - it is a directory or a link
  bool is_valid_disk_entry(struct dirent* pentry) const;
  const ObLogDiskInfo* get_first_good_disk() const;

  // Multiple disk startup process
  // 1. Detect new disk
  //   1) skip bad disk path
  //   2) if disk path doesn't exist, insert to an INVALID slot. Do initialize and change to NEW state.
  // 2. Use ObLogDirScanner to scan new disk
  //   1) Get the min and max file id
  //   2) Check if log is continuous
  //   3) Check is exist LOG_RESTORE file. If true, pick up the maximum log file from other disk.
  // 3. If disk contains LOG_RESTORE file, set to RESTORE state. The restore thread will do work.
  // 4. If disk doesn't contain LOG_RESTORE file, and log file is missing. This disk will be set
  //    to INVALID. Clean all the file under it and leave it to restore thread.
  // 5. For the other disk, copy the maximum file id from source disk and then become GOOD state.
  int startup();
  int clear_tmp_files();
  bool is_tmp_filename(const char* filename) const;
  int get_disk_file_range(
      ObIArray<DirScanResult>& results, int64_t& min_log_id, int64_t& max_log_id, int64_t& src_disk_id);
  int conform_disk_files(const ObIArray<DirScanResult>& results, const int64_t min_log_id, const int64_t max_log_id,
      const int64_t src_disk_id);
  // @brief: based on scan results, for all incomplete disks
  //         if it contains LOG_RESTORE flag file, set to RESTORE state
  //         otherwise, purge the whole disk and let monitor task detect it as a new empty disk
  int tackle_incomplete_disk(const ObIArray<DirScanResult>& results, const int64_t min_log_id, const int64_t max_log_id,
      const int64_t src_disk_id);
  int purge_disk_dir(const int64_t disk_id, const int64_t min_log_id, const int64_t max_log_id);
  int set_invalid_disk(const int64_t disk_id);
  // @brief: copy the max log file for NEW and RESTORE state disks.
  int clone_max_log_file(const ObIArray<DirScanResult>& results, const int64_t max_log_id, const int64_t src_disk_id);
  int inner_open_fd(const int64_t disk_id, const int64_t file_id, const int flag, const bool is_tmp, int& fd);
  // @brief: copy file content from src_fd to targets. The file content length is no more than
  //         file_offset
  // @param [in] src_fd: the source fd
  // @param [in] targets: the destination fd array
  // @param [in] file_offset: the end offset of copied content. If 0 means copy all file
  int copy_file_content(const ObLogFdInfo src_fd, const ObIArray<ObLogFdInfo>& targets, const int64_t file_offset);
  int inner_exist(const int64_t disk_id, const int64_t file_id, bool& exist) const;
  int inner_unlink(const int64_t disk_id, const int64_t file_id, const bool is_tmp) const;

  //  Add new disk process
  //  1. Detect new disk:
  //    1) not bad path,
  //    2) not exist path,
  //    3) find an empty slog and do init, set to NEW state
  //  2. Get the LogRestoreProgress
  //    1) NEW disk must be empty. Create LOG_RESTORE file and set the disk to RESTORE stat.
  //       From now on, this disk begin to accept new write files and us ObLogFileDescriptor to
  //       trace the sync progress.
  //    2) For RESTORE state, load the LOG_RESTORE file and get LogRestoreProgress
  //  3. Begin restore progress:
  //    1) pick a GOOD state disk, find the max_id and min_id
  //    2) According to LogRestoreProgress, find the restore file range
  //      a. If there's no restore progress, set LogRestoreProgress::copy_start_file_id_ = max_id - 1.
  //         flush the value to LOG_RESTORE file. The copy range is [min_id, max_id - 1]
  //      b. If has previous restore progress, read the copy range from it.
  //      When get the copy range, start to copy file one by one. Each time copied a file, update
  //      LogRestoreProgress::copied_file_id_ and flush it to LOG_RESTORE file.
  //      When all files copied, update LogRestoreProgress::copy_complete_ and flush it to LOG_RESTORE file
  //    3) According to LogRestoreProgress, judge if need to catch up the maximum file
  //      a. if already start catching up, read the begin offset from LOG_RESTORE file.
  //      b. if not start yet, get the the start offset from ObLogDiskInfo.
  //      Once finish catch up, update LogRestoreProgress::catchup_complete_ and flush it to LOG_RESTORE file.
  //  4. Delete LOG_RESTORE file and set disk state to GOOD.
  void run_monitor_task();
  int load_new_disks();
  int is_disk_dir_empty(const char* dir_path, bool& is_empty) const;
  int restore_log_files(const int64_t disk_id);
  // @brief: copy a single log file. Log files are copied for large to small, i.e.
  // file 2 is copied before file 1. The progress is
  //         1) create a tmp file based on file_id. fallocate file_size_ if it is CLOG
  //         2) copy whole content from src_disk_id
  //         3) check if src disk log file exist (maybe rename by log file reuse)
  //         4) if exist, delete current copied tmp file
  //            else, rename tmp file to file_id
  // @param [in] src_disk_id: source disk id
  // @param [in] dest_disk_id: destination disk id
  // @param [in] file_id: log file id
  // @param [in] copied: if TRUE, log file is copied; else log file is deleted due to reuse
  int copy_single_log_file(const int64_t src_disk_id, const int64_t dest_disk_id, const int64_t file_id, bool& copied);
  // @brief: in CLOG file reuse case, there could generate orphan file whose id is not continuous
  //         to others. For example: when copying file 3, file 4 is reused for new file 101. Then
  //         there's a hole between file 3 and file 5. File 3 should be cleared.
  int clear_orphan_files(const int64_t disk_id);
  // @brief: it is used to catchup the last n log files
  int catchup_log_files(const int64_t src_disk_id, const int64_t dest_disk_id);
  int sweep_bad_disk_slots();
  int remove_tmp_suffix(const int64_t disk_id, const int64_t file_id) const;

  // Create flag file when log files start catching up and delete it when complete.
  int log_restore_begin(const int64_t disk_id);
  int log_restore_end(const int64_t disk_id);
  int is_log_restoring(const int64_t disk_id, bool& restoring) const;
  int write_restore_progress();
  int get_total_disk_space_(int64_t& total_space) const;

  static const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  static const int OPEN_FLAG_WRITE = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  static const int OPEN_FLAG_WRITE_WITHOUT_CREATE = O_WRONLY | O_DIRECT | O_SYNC;
  static const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  static const int64_t MONITOR_TASK_INTERVAL_US = 1000 * 1000;    // 1 seconds
  static const int64_t BAD_DISK_RETENTION_US = 15 * 1000 * 1000;  // 15 seconds
  static const int64_t LOG_COPY_BUF_SIZE = DIO_READ_ALIGN_SIZE;
  static const int64_t RESTORE_PROGRESS_BUF_SIZE = DIO_ALIGN_SIZE;
  const char* BAD_DISK_SUFFIX = ".bad";
  const char* LOG_RESTORE_FILENAME = "LOG_RESTORE";
  const char* VALID_DISK_PREFIX = "disk";

private:
  bool is_inited_;
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  int64_t file_size_;
  int64_t total_disk_space_;
  clog::ObLogWritePoolType pool_type_;
  ObLogDiskInfo disk_slots_[MAX_DISK_COUNT];
  ObSpinLock init_lock_;

  int tg_id_;
  MonitorTask monitor_task_;
  char* log_copy_buffer_;
  char* rst_pro_buffer_;
  LogRestoreProgress rst_pro_;
  int rst_fd_;

  DISALLOW_COPY_AND_ASSIGN(ObLogDiskManager);
};

class ObSLogDiskManager : public ObLogDiskManager {
public:
  virtual ~ObSLogDiskManager()
  {}

  static ObSLogDiskManager& get_instance()
  {
    static ObSLogDiskManager instance_;
    return instance_;
  }

  virtual int init(const char* log_dir, const int64_t file_size)
  {
    return ObLogDiskManager::init(log_dir, file_size, clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  }

private:
  ObSLogDiskManager()
  {}
};

class ObCLogDiskManager : public ObLogDiskManager {
public:
  virtual ~ObCLogDiskManager()
  {}

  static ObCLogDiskManager& get_instance()
  {
    static ObCLogDiskManager instance_;
    return instance_;
  }

  virtual int init(const char* log_dir, const int64_t file_size)
  {
    return ObLogDiskManager::init(log_dir, file_size, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  }

private:
  ObCLogDiskManager()
  {}
};

class ObILogDiskManager : public ObLogDiskManager {
public:
  virtual ~ObILogDiskManager()
  {}

  static ObILogDiskManager& get_instance()
  {
    static ObILogDiskManager instance_;
    return instance_;
  }

  virtual int init(const char* log_dir, const int64_t file_size)
  {
    return ObLogDiskManager::init(log_dir, file_size, clog::ObLogWritePoolType::ILOG_WRITE_POOL);
  }

private:
  ObILogDiskManager()
  {}
};
}  // namespace common
}  // namespace oceanbase

#endif /* OB_LOG_DISK_MANAGER_H_ */
