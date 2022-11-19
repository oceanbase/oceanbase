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

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_TASK_
#define OCEANBASE_LOGSERVICE_LOG_IO_TASK_

#include "lib/utility/ob_print_utils.h"          // TO_STRING_KV
#include "lsn.h"                                 // LSN
#include "log_io_task_cb_utils.h"                // FlushLogCbCtx
#include "log_writer_utils.h"                    // LogWriteBuf

namespace oceanbase
{
namespace palf
{
class PalfHandleImpl;
class LogMeta;
class PalfEnvImpl;

enum class LogIOTaskType
{
  FLUSH_LOG_TYPE = 1,
  FLUSH_META_TYPE = 2,
  TRUNCATE_PREFIX_TYPE = 3,
  TRUNCATE_LOG_TYPE = 4
};

class LogIOTask
{
public:
  LogIOTask() : palf_epoch_(OB_INVALID_TIMESTAMP) {}
  virtual ~LogIOTask() {}

public:
  virtual int do_task(int tg_id, PalfEnvImpl *palf_env_impl) = 0;
  virtual int after_consume(PalfEnvImpl *palf_env_impl) = 0;
  virtual LogIOTaskType get_io_task_type() const = 0;
  virtual void free_this(PalfEnvImpl *palf_env_impl) = 0;
  int64_t get_palf_id() const { return palf_id_; }
  int64_t get_palf_epoch() const { return palf_epoch_; }
  VIRTUAL_TO_STRING_KV("LogIOTask", "dummy");

protected:
  int64_t palf_epoch_;
  int64_t palf_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogIOTask);
};

class LogIOFlushLogTask : public LogIOTask {
  friend class BatchLogIOFlushLogTask;
public:
  LogIOFlushLogTask();
  ~LogIOFlushLogTask() override;

public:
  int init(const FlushLogCbCtx &flush_log_cb_ctx,
           const LogWriteBuf &write_buf,
           const int64_t palf_id,
           const int64_t palf_epoch);
  void destroy();
  // IO thread will call this function to flush log
  int do_task(int tg_id, PalfEnvImpl *palf_env_impl) override final;
  // IO thread will call this function to submit async task
  int after_consume(PalfEnvImpl *palf_env_impl) override final;
  LogIOTaskType get_io_task_type() const override final { return LogIOTaskType::FLUSH_LOG_TYPE; }
  void free_this(PalfEnvImpl *palf_env_impl) override final;
  TO_STRING_KV(K_(write_buf), K_(flush_log_cb_ctx), K_(is_inited), K_(palf_epoch));

private:
  FlushLogCbCtx flush_log_cb_ctx_;
  LogWriteBuf write_buf_;
  bool is_inited_;
};

class LogIOTruncateLogTask : public LogIOTask {
public:
  LogIOTruncateLogTask();
  ~LogIOTruncateLogTask() override;

public:
  int init(const TruncateLogCbCtx &truncate_log_cb_ctx,
           const int64_t palf_id,
           const int64_t palf_epoch);
  void destroy();
  int do_task(int tg_id, PalfEnvImpl *palf_env_impl) override final;
  int after_consume(PalfEnvImpl *palf_env_impl) override final;
  LogIOTaskType get_io_task_type() const override final { return LogIOTaskType::TRUNCATE_LOG_TYPE; }
  void free_this(PalfEnvImpl *palf_env_impl) override final;
  TO_STRING_KV(K_(truncate_log_cb_ctx), K_(palf_id), K_(palf_epoch));
private:
  TruncateLogCbCtx truncate_log_cb_ctx_;
  bool is_inited_;
};

class LogIOFlushMetaTask : public LogIOTask {
public:
  LogIOFlushMetaTask();
  ~LogIOFlushMetaTask() override;

public:
  int init(const FlushMetaCbCtx &flush_cb_ctx,
           const char *buf,
           const int64_t buf_len,
           const int64_t palf_id,
           const int64_t palf_epoch);
  void destroy();
  int do_task(int tg_id, PalfEnvImpl *palf_env_impl) override final;
  int after_consume(PalfEnvImpl *palf_env_impl) override final;
  LogIOTaskType get_io_task_type() const override final { return LogIOTaskType::FLUSH_META_TYPE; }
  void free_this(PalfEnvImpl *palf_env_impl) override final;
  TO_STRING_KV(K_(flush_meta_cb_ctx), K_(palf_id), K_(is_inited), K_(palf_epoch));

private:
  FlushMetaCbCtx flush_meta_cb_ctx_;
  const char *buf_;
  int64_t buf_len_;
  int64_t palf_id_;
  int64_t palf_epoch_;
  bool is_inited_;
};

class LogIOTruncatePrefixBlocksTask : public LogIOTask {
public:
  LogIOTruncatePrefixBlocksTask();
  ~LogIOTruncatePrefixBlocksTask();
public:
  int init(const TruncatePrefixBlocksCbCtx& truncate_prefix_blocks_ctx,
           const int64_t palf_id,
           const int64_t palf_epoch);
  void destroy();
  int do_task(int tg_id, PalfEnvImpl *palf_env_impl) override final;
  int after_consume(PalfEnvImpl *palf_env_impl) override final;
  LogIOTaskType get_io_task_type() const override final { return LogIOTaskType::TRUNCATE_PREFIX_TYPE; }
  void free_this(PalfEnvImpl *palf_env_impl) override final;
  TO_STRING_KV(K_(truncate_prefix_blocks_ctx), K_(palf_id), K_(palf_epoch));
private:
  TruncatePrefixBlocksCbCtx truncate_prefix_blocks_ctx_;
  bool is_inited_;
};

// BatchLogIOFlushLogTask is the set of LogIOFlushLogTask
class BatchLogIOFlushLogTask {
public:
  BatchLogIOFlushLogTask();
  ~BatchLogIOFlushLogTask();
public:
  typedef common::ObFixedArray<LogIOFlushLogTask *, ObIAllocator> BatchIOTaskArray;
  int init(const int64_t batch_depth, ObIAllocator *allocator);
  void reuse();
  void destroy();
  int push_back(LogIOFlushLogTask *task);
  int do_task(int tg_id, PalfEnvImpl *palf_env_impl);
  int64_t get_palf_id() const { return palf_id_; }
  int64_t get_count() const { return io_task_array_.count(); }
  TO_STRING_KV(K_(palf_id), "count", io_task_array_.count(), K_(lsn_array));
private:
  int push_flush_cb_to_thread_pool_(int tg_id, PalfEnvImpl *palf_env_impl);
  int do_task_(int tg_id, PalfEnvImpl *palf_env_impl);
  void clear_memory_(PalfEnvImpl *palf_env_impl);
private:
  BatchIOTaskArray io_task_array_;
  LogWriteBufArray log_write_buf_array_;
  LogTsArray log_ts_array_;
  LSNArray lsn_array_;
  int64_t palf_id_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
