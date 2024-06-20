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
class IPalfEnvImpl;

enum class LogIOTaskType
{
  FLUSH_LOG_TYPE = 1,
  FLUSH_META_TYPE = 2,
  TRUNCATE_PREFIX_TYPE = 3,
  TRUNCATE_LOG_TYPE = 4,
	FLASHBACK_LOG_TYPE = 5,
  PURGE_THROTTLING_TYPE = 6,
};

class IPalfHandleImplGuard;
class LogIOTask;

int push_task_into_cb_thread_pool(const int64_t tg_id, LogIOTask *io_task);

class LogIOTask
{
public:
	LogIOTask(const int64_t palf_id, const int64_t palf_epoch);
  virtual ~LogIOTask();
	void reset();
public:
  int do_task(int tg_id, IPalfEnvImpl *palf_env_impl);
  int after_consume(IPalfEnvImpl *palf_env_impl);
  LogIOTaskType get_io_task_type();
  int64_t get_init_task_ts();
  void free_this(IPalfEnvImpl *palf_env_impl);
  int64_t get_io_size();
  int64_t get_palf_id() const { return palf_id_; }
  int64_t get_submit_seq() const {return submit_seq_;}
  void set_submit_seq(const int64_t submit_seq) {submit_seq_ = submit_seq;}
  bool need_purge_throttling();
  int64_t get_palf_epoch() const { return palf_epoch_; }
	VIRTUAL_TO_STRING_KV("BaseClass", "LogIOTask",
			"palf_id", palf_id_,
			"palf_epoch", palf_epoch_,
			"create_task_ts", init_task_ts_,
			"push_cb_into_cb_pool_ts", push_cb_into_cb_pool_ts_,
      K(submit_seq_));

protected:
  virtual int do_task_(int tg_id, IPalfHandleImplGuard &guard) = 0;
  virtual int after_consume_(IPalfHandleImplGuard &guard) = 0;
  virtual LogIOTaskType get_io_task_type_() const = 0;
  virtual void free_this_(IPalfEnvImpl *palf_env_impl) = 0;
  virtual int64_t get_io_size_() const = 0;
  virtual bool need_purge_throttling_() const = 0;
	int push_task_into_cb_thread_pool_(const int64_t tg_id, LogIOTask *io_task);

protected:
  int64_t palf_id_;
  int64_t palf_epoch_;
	int64_t init_task_ts_;
	int64_t push_cb_into_cb_pool_ts_;
  //used for writing throttling
  int64_t submit_seq_;

private:
  DISALLOW_COPY_AND_ASSIGN(LogIOTask);
};

class LogIOFlushLogTask : public LogIOTask {
  friend class BatchLogIOFlushLogTask;
public:
  LogIOFlushLogTask(const int64_t palf_id,const int64_t palf_epoch);
  ~LogIOFlushLogTask() override;

  int init(const FlushLogCbCtx &flush_log_cb_ctx,
           const LogWriteBuf &write_buf);
  void destroy();
  INHERIT_TO_STRING_KV("LogIOTask", LogIOTask, K_(write_buf), K_(flush_log_cb_ctx), K(is_inited_));
private:
  // IO thread will call this function to flush log
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  // IO thread will call this function to submit async task
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::FLUSH_LOG_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final;
  bool need_purge_throttling_() const override final {return false;}

private:
  FlushLogCbCtx flush_log_cb_ctx_;
  LogWriteBuf write_buf_;
  bool is_inited_;
};

class LogIOTruncateLogTask : public LogIOTask {
public:
  LogIOTruncateLogTask(const int64_t palf_id,const int64_t palf_epoch);
  ~LogIOTruncateLogTask() override;

  int init(const TruncateLogCbCtx &truncate_log_cb_ctx);
  void destroy();

  INHERIT_TO_STRING_KV("LogIOTask", LogIOTask, K_(truncate_log_cb_ctx));
private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::TRUNCATE_LOG_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final {return 0;}
  bool need_purge_throttling_() const override final {return false;}
private:
  TruncateLogCbCtx truncate_log_cb_ctx_;
  bool is_inited_;
};

class LogIOFlushMetaTask : public LogIOTask {
public:
  LogIOFlushMetaTask(const int64_t palf_id,const int64_t palf_epoch);
  ~LogIOFlushMetaTask() override;

public:
  int init(const FlushMetaCbCtx &flush_cb_ctx,
           const char *buf,
           const int64_t buf_len);
  void destroy();
  INHERIT_TO_STRING_KV("LogIOTask", LogIOTask, K_(flush_meta_cb_ctx));

private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::FLUSH_META_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final {return buf_len_;}
  bool need_purge_throttling_() const override final {return true;}

private:
  FlushMetaCbCtx flush_meta_cb_ctx_;
  const char *buf_;
  int64_t buf_len_;
  bool is_inited_;
};

class LogIOTruncatePrefixBlocksTask : public LogIOTask {
public:
  LogIOTruncatePrefixBlocksTask(const int64_t palf_id,const int64_t palf_epoch);
  ~LogIOTruncatePrefixBlocksTask();

  int init(const TruncatePrefixBlocksCbCtx& truncate_prefix_blocks_ctx);
  void destroy();

  INHERIT_TO_STRING_KV("LogIOTask", LogIOTask, K_(truncate_prefix_blocks_ctx));
private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::TRUNCATE_PREFIX_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final {return 0;}
  bool need_purge_throttling_() const override final {return false;}
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
  void get_io_task_array(BatchIOTaskArray& io_task_array) { io_task_array_ = io_task_array_; }
  int do_task(int tg_id, IPalfEnvImpl *palf_env_impl);
  void reset_accum_in_queue_time() { accum_in_queue_time_ = 0; }
  int64_t get_palf_id() const { return palf_id_; }
  int64_t get_count() const { return io_task_array_.count(); }
  int64_t get_accum_in_queue_time() const { return accum_in_queue_time_; }

  TO_STRING_KV(K_(palf_id), "count", io_task_array_.count(), K_(lsn_array), K_(accum_size));
private:
  int push_flush_cb_to_thread_pool_(int tg_id, IPalfEnvImpl *palf_env_impl);
  int do_task_(int tg_id, IPalfEnvImpl *palf_env_impl);
  void clear_memory_(IPalfEnvImpl *palf_env_impl);
  static int64_t SINGLE_TASK_MAX_SIZE;
private:
  BatchIOTaskArray io_task_array_;
  LogWriteBufArray log_write_buf_array_;
  SCNArray scn_array_;
  LSNArray lsn_array_;
  int64_t palf_id_;
  int64_t accum_in_queue_time_;
  int64_t accum_size_;
  bool is_inited_;
};

class LogIOFlashbackTask : public LogIOTask
{
public:
  LogIOFlashbackTask(const int64_t palf_id,const int64_t palf_epoch);
  ~LogIOFlashbackTask();
public:
  int init(const FlashbackCbCtx & flashback_ctx,
           const int64_t palf_id);
  void destroy();
  TO_STRING_KV(K_(palf_id), K_(flashback_ctx), K_(is_inited));
private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::FLASHBACK_LOG_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final {return 0;}
  bool need_purge_throttling_() const override final {return true;}
private:
  FlashbackCbCtx flashback_ctx_;
  bool is_inited_;
};

class LogIOPurgeThrottlingTask : public LogIOTask {
public:
  LogIOPurgeThrottlingTask(const int64_t palf_id, const int64_t palf_epoch);
  ~LogIOPurgeThrottlingTask();
  void reset();
public:
  int init(const PurgeThrottlingCbCtx & flashback_ctx);
  void destroy();
  INHERIT_TO_STRING_KV("LogIOTask", LogIOTask, K_(purge_ctx), K_(is_inited));
private:
  int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
  int after_consume_(IPalfHandleImplGuard &guard) override final;
  LogIOTaskType get_io_task_type_() const override final { return LogIOTaskType::PURGE_THROTTLING_TYPE; }
  void free_this_(IPalfEnvImpl *palf_env_impl) override final;
  int64_t get_io_size_() const override final {return 0;}
  bool need_purge_throttling_() const override final {return true;}
private:
  PurgeThrottlingCbCtx purge_ctx_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
