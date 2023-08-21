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
#ifndef OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_IO_TASK_H_
#define OCEABASE_LOGSERVICE_OB_LOG_EXTERNAL_STORAGE_IO_TASK_H_
#include "lib/utility/ob_macro_utils.h"                     // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"                     // TO_STRING_KV
#include "lib//ob_define.h"                                 // OB_MAX_URI_LENGTH
#include "lib/lock/ob_thread_cond.h"                        // ObThreadCond
#include "share/backup/ob_backup_struct.h"                  // OB_MAX_BACKUP_STORAGE_INFO_LENGTH
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace logservice
{
enum class EnumRunningStatus {
  INVALID_STATUS = 0,
  START_STATUS = 1,
  FINSHED_STATUS = 2,
  MAX_STATUS = 3
};

struct RunningStatus
{
  RunningStatus();
  ~RunningStatus();
  void reset();
  int ret_;
  // For debug
  int64_t main_thread_id_;
  int64_t thread_id_;
  int64_t logical_thread_id_;
  EnumRunningStatus status_;
  TO_STRING_KV(KR(ret_), K_(main_thread_id), K_(thread_id), K_(logical_thread_id), K_(status));
};

class ObLogExternalStorageIOTaskCtx {
public:
  ObLogExternalStorageIOTaskCtx();
  ~ObLogExternalStorageIOTaskCtx();

public:
  int init(const int64_t total_task_count);
  void destroy();
  // @brief: called by the executer of ObLogExternalStorageIOTask after it has finished.
  void signal();
  // @brief: wait up to timeout_us until there is no flying task.
  // @return value
  //   OB_SUCCESS
  //   OB_TIMEOUT, timeout
  //   others, system error
  int wait(int64_t timeout_us);

  int get_running_status(const int64_t idx,
                         RunningStatus *&running_stats) const;
  int get_ret_code() const;
  bool has_flying_async_task() const;
  DECLARE_TO_STRING;

private:
  int construct_running_status_(const int64_t total_task_count);
  void deconstruct_running_status_();
private:
  int64_t flying_task_count_;
  int64_t total_task_count_;
  mutable ObThreadCond condition_;
  RunningStatus *running_status_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExternalStorageIOTaskCtx);
};

enum class ObLogExternalStorageIOTaskType {
  INVALID_TYPE = 0,
  PREAD_TYPE = 1,
  MAX_TYPE = 2
};

class ObLogExternalStorageIOTaskHandleIAdapter {
public:
  ObLogExternalStorageIOTaskHandleIAdapter() {}
  virtual ~ObLogExternalStorageIOTaskHandleIAdapter() {}

  // Interface
public:

  virtual int exist(const common::ObString &uri,
                    const common::ObString &storage_info,
                    bool &exist) = 0;

  virtual int get_file_size(const common::ObString &uri,
                            const common::ObString &storage_info,
                            int64_t &file_size) = 0;

  virtual int pread(const common::ObString &uri,
                    const common::ObString &storage_info,
                    const int64_t offset,
                    char *buf,
                    const int64_t read_buf_size,
                    int64_t &real_read_size) = 0;
};

class ObLogExternalStorageIOTaskHandleAdapter : public ObLogExternalStorageIOTaskHandleIAdapter {
public:
  ObLogExternalStorageIOTaskHandleAdapter();
  ~ObLogExternalStorageIOTaskHandleAdapter() override;

  // Implement
public:

  int exist(const common::ObString &uri,
            const common::ObString &storage_info,
            bool &exist) override final;

  int get_file_size(const common::ObString &uri,
                    const common::ObString &storage_info,
                    int64_t &file_size) override final;

  int pread(const common::ObString &uri,
            const common::ObString &storage_info,
            const int64_t offset,
            char *buf,
            const int64_t read_buf_size,
            int64_t &real_read_size) override final;
};

class ObLogExternalStorageIOTask {
public:
  ObLogExternalStorageIOTask(const ObString &uri,
                             const ObString &storage_info,
                             RunningStatus *running_status,
                             ObLogExternalStorageIOTaskCtx *io_task_ctx,
                             ObLogExternalStorageIOTaskHandleIAdapter *adapter);

  virtual ~ObLogExternalStorageIOTask();

public:
  int do_task();

	VIRTUAL_TO_STRING_KV("BaseClass", "ObLogExternalStorageIOTask",
			"uri", uri_ob_str_,
			"storage_info", storage_info_ob_str_,
			"generate_ts", generate_ts_,
			"do_task_ts", do_task_ts_,
	    "type", type_,
      KP(running_status_),
      KPC(io_task_ctx_));
private:
  virtual int do_task_() = 0;

protected:
  ObString uri_ob_str_;
  char uri_str_[OB_MAX_URI_LENGTH];
  ObString storage_info_ob_str_;
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  int64_t generate_ts_;
  int64_t do_task_ts_;
  RunningStatus *running_status_;
  ObLogExternalStorageIOTaskCtx *io_task_ctx_;
  ObLogExternalStorageIOTaskType type_;
  ObLogExternalStorageIOTaskHandleIAdapter *adapter_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExternalStorageIOTask);
};

class ObLogExternalStoragePreadTask : public ObLogExternalStorageIOTask {
public:
  ObLogExternalStoragePreadTask(const ObString &uri,
                                const ObString &storage_info,
                                RunningStatus *running_status,
                                ObLogExternalStorageIOTaskCtx *io_task_ctx,
                                ObLogExternalStorageIOTaskHandleIAdapter *adapter,
                                const int64_t offset,
                                const int64_t read_buf_size,
                                char *read_buf,
                                int64_t &real_read_size);
  ~ObLogExternalStoragePreadTask() override;

public:
  INHERIT_TO_STRING_KV("ObLogExternalStorageTask", ObLogExternalStorageIOTask,
                       K_(offset), K_(read_buf_size), KP(read_buf_), K_(real_read_size));
private:

  int do_task_() override final;

private:
  int64_t offset_;
  int64_t read_buf_size_;
  char *read_buf_;
  // shared variable between different threads, need use atomic operation.
  int64_t &real_read_size_;
};
} // end namespace logservice
} // end namespace oceanbase

#endif
