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

#ifndef OCEANBASE_LOG_MINER_BR_PRODUCER_H_
#define OCEANBASE_LOG_MINER_BR_PRODUCER_H_

#include "ob_log_miner_br.h"
#include "ob_log_instance.h"
#include "ob_log_miner_error_handler.h"
#include "ob_log_miner_args.h"
#include <map>
namespace oceanbase
{
namespace oblogminer
{

class ILogMinerBRFilter;

class ILogMinerBRProducer
{
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
};

class ObLogMinerBRProducer :public ILogMinerBRProducer, public lib::ThreadPool
{
public:
  static const int64_t BR_PRODUCER_THREAD_NUM;
  static const int64_t BR_PRODUCER_POOL_DEFAULT_NUM;
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  // need to be idempotent
  virtual void destroy();

private:
  virtual void run1() override;

public:
  ObLogMinerBRProducer();
  ~ObLogMinerBRProducer()
  { destroy(); }
  int init(const AnalyzerArgs &args,
      ILogMinerBRFilter *filter,
      ILogMinerErrorHandler *err_handle);

private:
  int build_cdc_config_map_(const AnalyzerArgs &args,
      std::map<std::string, std::string> &cdc_config);
  int get_tenant_compat_mode_(const RecordType type,
      const uint64_t tenant_id,
      lib::Worker::CompatMode &mode);

  int set_current_trans_id_(ICDCRecord *cdc_rec);
private:
  bool is_inited_;
  bool is_dispatch_end_;
  int64_t start_time_us_;
  int64_t end_time_us_;
  transaction::ObTransID curr_trans_id_;
  libobcdc::ObCDCFactory cdc_factory_;
  libobcdc::IObCDCInstance *cdc_instance_;
  ILogMinerBRFilter *br_filter_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif