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
 *
 * Fetcher Data Dispatcher
 * After the data is generated (PartTransTask) in Fetcher, it is distributed through the data distributor
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER

#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN, CACHE_ALIGNED

#include "ob_log_fetcher_dispatcher_interface.h" // IObLogFetcherDispatcher
#include "ob_log_utils.h"                       // _SEC_

namespace oceanbase
{
namespace libobcdc
{

/////////////////////////////////////////////////////////////////////////////////

class IObLogDmlParser;
class IObLogSysLsTaskHandler;
class IObLogCommitter;
class ObLogFetcherDispatcher : public IObLogFetcherDispatcher
{
  static const int64_t DATA_OP_TIMEOUT = 10 * _SEC_;

public:
  ObLogFetcherDispatcher();
  virtual ~ObLogFetcherDispatcher();

  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag);

public:
  int init(
      IObLogSysLsTaskHandler *sys_ls_handler,
      IObLogCommitter *committer,
      const int64_t start_seq);
  void destroy();

private:
  int dispatch_dml_trans_task_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_ddl_trans_task_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_ls_heartbeat_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_to_committer_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_offline_ls_task_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_global_ls_heartbeat_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_ls_table_op_(PartTransTask &task, volatile bool &stop_flag);
  int dispatch_to_sys_ls_handler_(PartTransTask &task, volatile bool &stop_flag);

private:
  bool              inited_;
  IObLogSysLsTaskHandler *sys_ls_handler_;
  IObLogCommitter   *committer_;

  // DML and Global HeartBeat checkpoint seq
  // DDL global checkpoint seq:
  // 1. DDL trans
  // 2. DDL HeartBeat
  // 3. DDL Offline Task
  int64_t           checkpoint_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherDispatcher);
};

}
}

#endif
