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

#ifndef OCEANBASE_LOG_MINER_BR_FILTER_H_
#define OCEANBASE_LOG_MINER_BR_FILTER_H_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "libobcdc.h"
#include "ob_log_miner_filter_condition.h"
#include "ob_log_miner_resource_collector.h"
#include "lib/container/ob_se_array.h"
#include "ob_log_miner_error_handler.h"

namespace oceanbase
{
namespace oblogminer
{
class ObLogMinerBR;
class ILogMinerBRProducer;
class ILogMinerBRConverter;
class ILogMinerDataManager;

class IBRFilterPlugin
{
public:
  virtual int filter(ObLogMinerBR &br, bool &need_filter) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ColumnBRFilterPlugin: public IBRFilterPlugin
{
public:
  virtual int filter(ObLogMinerBR &br, bool &need_filter) override;
  TO_STRING_KV(K_(multi_table_column_cond));
public:
  ColumnBRFilterPlugin(ObIAllocator *alloc);
  ~ColumnBRFilterPlugin() { destroy(); };
  int init(const char *table_cond_str);
  void destroy();


private:
  bool need_process_(const RecordType type);
  bool is_table_match_(const ObLogMinerTableColumnCond& table_cond,
       const char *db_name,
       const char *table_name);
  bool satisfy_column_cond_(const ObLogMinerColumnVals &col_cond,
       const binlogBuf *new_cols,
       const int64_t new_col_cnt,
       const binlogBuf *old_cols,
       const int64_t old_col_cnt,
       ITableMeta *tbl_meta);
  bool is_data_match_column_val_(const char *data_ptr,
       const int64_t data_len,
       const ObLogMinerColVal &col_val);

private:
  ObIAllocator *alloc_;
  ObLogMinerMultiTableColumnCond multi_table_column_cond_;
};

class OperationBRFilterPlugin: public IBRFilterPlugin
{
public:
  virtual int filter(ObLogMinerBR &br, bool &need_filter) override;
  TO_STRING_KV(K_(op_cond));
public:
  OperationBRFilterPlugin();
  ~OperationBRFilterPlugin() { destroy(); }
  int init(const char *op_cond_str);
  void destroy();

private:
  ObLogMinerOpCond op_cond_;
};

class ILogMinerBRFilter {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int push(ObLogMinerBR *logminer_br) = 0;
  virtual int get_total_task_count(int64_t &task_count) = 0;
};

typedef ObMQThread<1, ILogMinerBRFilter> BRFilterThreadPool;

class ObLogMinerBRFilter: public ILogMinerBRFilter, public BRFilterThreadPool
{
public:
  static const int64_t BR_FILTER_THREAD_NUM;
  static const int64_t BR_FILTER_QUEUE_SIZE;
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int push(ObLogMinerBR *logminer_br);
  virtual int get_total_task_count(int64_t &task_count);

public:
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  ObLogMinerBRFilter();
  ~ObLogMinerBRFilter();
  int init(const char *table_cond,
      const char *op_cond,
      ILogMinerDataManager *data_manager,
      ILogMinerResourceCollector *resource_collector,
      ILogMinerBRConverter *br_converter,
      ILogMinerErrorHandler *err_handle);

private:
  int filter_br_(ObLogMinerBR &br, bool &need_filter);

private:
  bool is_inited_;
  ObArenaAllocator plugin_allocator;
  common::ObSEArray<IBRFilterPlugin *, 4> filter_pipeline_;
  ILogMinerDataManager *data_manager_;
  ILogMinerResourceCollector *resource_collector_;
  ILogMinerBRConverter *br_converter_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif