/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_SCHEDULER_H_
#define SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_SCHEDULER_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_storage_io_pipeline.h"
#include "storage/macro_cache/ob_ext_table_disk_cache_common_meta.h"

namespace oceanbase
{
namespace sql
{

bool is_valid_external_macro_id(const blocksstable::MacroBlockId &macro_id);

class ObExternalDataPrefetchTaskInfo : public storage::TaskInfoWithRWHandle
{
public:
  enum PrefetchOperationType
  {
    READ_THEN_WRITE,
    DIRECT_WRITE,
    MAX_TYPE
  };

public:
  ObExternalDataPrefetchTaskInfo();
  virtual ~ObExternalDataPrefetchTaskInfo() { ObExternalDataPrefetchTaskInfo::reset(); }

  int assign(const ObExternalDataPrefetchTaskInfo &other);
  virtual void reset() override;
  virtual bool is_valid() const override;

  int init(
      const common::ObString &url,
      const common::ObObjectStorageInfo *info,
      const int64_t read_offset,
      const blocksstable::MacroBlockId &macro_id);
  int init(
      const char *data,
      const int64_t data_size,
      const blocksstable::MacroBlockId &macro_id);
  virtual uint64_t hash() const = 0;

  INHERIT_TO_STRING_KV("TaskInfoWithRWHandle", storage::TaskInfoWithRWHandle,
      K(url_), KPC(storage_info_), K(read_offset_), K(macro_id_), K(operation_type_));

protected:
  virtual int refresh_read_state_() override;

public:
  common::ObArenaAllocator allocator_;
  PrefetchOperationType operation_type_;
  blocksstable::MacroBlockId macro_id_;

  // for READ_THEN_WRITE
  char url_[common::OB_MAX_URI_LENGTH];
  common::ObObjectStorageInfo *storage_info_;
  int64_t read_offset_;

  // for DIRECT_WRITE
  // data size must be equal to macro block size
  char *data_;
};

class ObSSExternalDataPrefetchTaskInfo : public ObExternalDataPrefetchTaskInfo
{
public:
  virtual uint64_t hash() const override;
};

class ObSNExternalDataPrefetchTaskInfo : public ObExternalDataPrefetchTaskInfo
{
public:
  ObSNExternalDataPrefetchTaskInfo();
  virtual ~ObSNExternalDataPrefetchTaskInfo() { ObSNExternalDataPrefetchTaskInfo::reset(); }

  int assign(const ObSNExternalDataPrefetchTaskInfo &other);
  virtual void reset() override;
  virtual bool is_valid() const override;

  int init(
      const common::ObString &url,
      const common::ObObjectStorageInfo *info,
      const int64_t read_offset,
      const blocksstable::MacroBlockId &macro_id,
      const storage::ObExtFileVersion &file_version);
  int init(
      const char *data,
      const int64_t data_size,
      const common::ObString &url,
      const int64_t read_offset,
      const blocksstable::MacroBlockId &macro_id,
      const storage::ObExtFileVersion &file_version);
  virtual uint64_t hash() const override;

  INHERIT_TO_STRING_KV("ObExternalDataPrefetchTaskInfo", ObExternalDataPrefetchTaskInfo,
      K(content_digest_), K(modify_time_));

public:
  common::ObSmallString<64> content_digest_;
  int64_t modify_time_;
};

template<typename TaskInfoType_>
class ObExternalDataPrefetchPipeline
    : public storage::ObStorageIOPipeline<TaskInfoType_>
{
public:
  static_assert(std::is_base_of<ObExternalDataPrefetchTaskInfo, TaskInfoType_>::value,
      "TaskInfoType_ must be derived from ObExternalDataPrefetchTaskInfo");
  using typename storage::ObStorageIOPipeline<TaskInfoType_>::TaskType;

public:
  ObExternalDataPrefetchPipeline();
  virtual ~ObExternalDataPrefetchPipeline();

  int init(const uint64_t tenant_id);

protected:
  virtual int on_task_failed_(TaskType &task) override;
  virtual int on_task_succeeded_(TaskType &task) override;
  virtual int do_async_read_(TaskType &task) override;
  virtual int do_async_write_(TaskType &task) override;
  virtual int do_complete_task_(TaskType &task) override;
};

using ObSSExternalDataPrefetchPipeline = ObExternalDataPrefetchPipeline<ObSSExternalDataPrefetchTaskInfo>;
using ObSNExternalDataPrefetchPipeline = ObExternalDataPrefetchPipeline<ObSNExternalDataPrefetchTaskInfo>;


class ObPCachedExtServicePrefetchTimerBase : public common::ObTimerTask
{
public:
  ObPCachedExtServicePrefetchTimerBase() {}
  virtual ~ObPCachedExtServicePrefetchTimerBase() {}
  virtual void destroy() = 0;

  virtual int init(const uint64_t tenant_id) = 0;
  virtual int add_task(const ObSSExternalDataPrefetchTaskInfo &task_info) = 0;
  virtual int add_task(const ObSNExternalDataPrefetchTaskInfo &task_info) = 0;
  virtual void stop() = 0;
};

template<typename PipelineType>
class ObPCachedExtServicePrefetchTimerTask : public ObPCachedExtServicePrefetchTimerBase
{
public:
  ObPCachedExtServicePrefetchTimerTask();
  virtual ~ObPCachedExtServicePrefetchTimerTask() { destroy(); }
  virtual void destroy() override;

  virtual int init(const uint64_t tenant_id) override;
  virtual void runTimerTask() override;
  virtual int add_task(const ObSSExternalDataPrefetchTaskInfo &task_info) override;
  virtual int add_task(const ObSNExternalDataPrefetchTaskInfo &task_info) override;
  virtual void stop() override;

  TO_STRING_KV(K(is_inited_), K(pipeline_));

private:
  bool is_inited_;
  PipelineType pipeline_;
};

class ObPCachedExtServiceExpireTask : public common::ObTimerTask
{
public:
  ObPCachedExtServiceExpireTask();
  virtual ~ObPCachedExtServiceExpireTask();

  virtual void runTimerTask() override;

private:
  static const int64_t EXPIRATION_DURATION_US = 3LL * common::DAY_US; // 3day
};

class ObPCachedExtServiceCleanupTask : public common::ObTimerTask
{
public:
  ObPCachedExtServiceCleanupTask();
  virtual ~ObPCachedExtServiceCleanupTask();
  virtual void runTimerTask() override;
};

class ObPCachedExtServiceStatTask : public common::ObTimerTask
{
public:
  ObPCachedExtServiceStatTask();
  virtual ~ObPCachedExtServiceStatTask();
  virtual void runTimerTask() override;
};

class ObPCachedExternalFileService;
class ObPCachedExtServiceScheduler
{
public:
  friend class ObPCachedExternalFileService;

public:
  ObPCachedExtServiceScheduler();
  ~ObPCachedExtServiceScheduler() { destroy(); }

  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(is_stopped), K_(tg_id),
      K(prefetch_timer_));

private:
  int schedule_task_(common::ObTimerTask &task, const int64_t interval_us);

private:
  static const int64_t PREFETCH_TASK_SCHEDULE_INTERVAL_US = 50LL * common::MS_US; // 50ms
  static const int64_t EXPIRE_TASK_SCHEDULE_INTERVAL_US = common::DAY_US; // 24h
  static const int64_t CLEANUP_TASK_SCHEDULE_INTERVAL_US = common::DAY_US; // 24h
  static const int64_t STAT_TASK_SCHEDULE_INTERVAL_US = 60LL * common::S_US; // 60s

  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int tg_id_;

private:
  ObPCachedExtServicePrefetchTimerTask<ObSSExternalDataPrefetchPipeline> ss_prefetch_timer_;
  ObPCachedExtServicePrefetchTimerTask<ObSNExternalDataPrefetchPipeline> sn_prefetch_timer_;
  ObPCachedExtServicePrefetchTimerBase &prefetch_timer_;
  // ObPCachedExtServiceExpireTask expire_task_;  disable expire task
  ObPCachedExtServiceCleanupTask cleanup_task_;
  ObPCachedExtServiceStatTask stat_task_;
};

} // sql
} // oceanbase

#endif // SQL_ENGIENE_TABLE_OB_PCACHED_EXTERNAL_FILE_SERVICE_SCHEDULER_H_