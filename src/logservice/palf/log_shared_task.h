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

#ifndef OCEANBASE_LOGSERVICE_LOG_SHARED_TASK_
#define OCEANBASE_LOGSERVICE_LOG_SHARED_TASK_

#include "lib/utility/ob_print_utils.h"
#include "lsn.h"
namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;

enum class LogSharedTaskType
{
  LogHandleSubmitType = 1,
  LogFillCacheType = 2,
};

inline const char *shared_type_2_str(const LogSharedTaskType type)
{
#define EXTRACT_SHARED_TYPE(type_var) ({ case(LogSharedTaskType::type_var): return #type_var; })
  switch(type)
  {
    EXTRACT_SHARED_TYPE(LogHandleSubmitType);
    EXTRACT_SHARED_TYPE(LogFillCacheType);
    default:
      return "Invalid Type";
  }
#undef EXTRACT_SHARED_TYPE
}

class LogSharedTask
{
public:
  LogSharedTask(const int64_t palf_id, const int64_t palf_epoch);
  virtual ~LogSharedTask();
  void destroy();
  void reset();
  virtual int do_task(IPalfEnvImpl *palf_env_impl) = 0;
  virtual void free_this(IPalfEnvImpl *palf_env_impl) = 0;
  virtual LogSharedTaskType get_shared_task_type() const = 0;
  VIRTUAL_TO_STRING_KV("BaseClass", "LogSharedTask",
      "palf_id", palf_id_,
      "palf_epoch", palf_epoch_);
protected:
  int64_t palf_id_;
  int64_t palf_epoch_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogSharedTask);
};

class LogHandleSubmitTask : public LogSharedTask
{
public:
  LogHandleSubmitTask(const int64_t palf_id, const int64_t palf_epoch);
  ~LogHandleSubmitTask() override;
  int do_task(IPalfEnvImpl *palf_env_impl) override;
  void free_this(IPalfEnvImpl *palf_env_impl) override;
  virtual LogSharedTaskType get_shared_task_type() const override { return LogSharedTaskType::LogHandleSubmitType; }
  INHERIT_TO_STRING_KV("LogSharedTask", LogSharedTask, "task type", shared_type_2_str(get_shared_task_type()));
private:
  DISALLOW_COPY_AND_ASSIGN(LogHandleSubmitTask);
};

class LogFillCacheTask : public LogSharedTask
{
public:
  LogFillCacheTask(const int64_t palf_id, const int64_t palf_epoch);
  ~LogFillCacheTask() override;
  int init(const LSN &begin_lsn, const int64_t size);
  int do_task(IPalfEnvImpl *palf_env_impl) override;
  void free_this(IPalfEnvImpl *palf_env_impl) override;
  virtual LogSharedTaskType get_shared_task_type() const override { return LogSharedTaskType::LogFillCacheType; }
  INHERIT_TO_STRING_KV("LogSharedTask", LogSharedTask, "task type", shared_type_2_str(get_shared_task_type()));
private:
  bool is_inited_;
  LSN begin_lsn_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(LogFillCacheTask);
};

} // end namespace palf
} // end namespace oceanbase

#endif
