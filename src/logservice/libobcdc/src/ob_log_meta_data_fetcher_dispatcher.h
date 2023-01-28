/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * LogMetaDataService Fetcher Data Dispatcher
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_META_DATA_FETCHER_DISPATCHER
#define OCEANBASE_LIBOBCDC_OB_LOG_META_DATA_FETCHER_DISPATCHER

#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN, CACHE_ALIGNED
#include "ob_log_fetcher_dispatcher_interface.h"  // IObLogFetcherDispatcher
#include "ob_log_utils.h"
#include "ob_log_meta_data_replayer.h"

namespace oceanbase
{
namespace libobcdc
{
class ObLogMetaDataFetcherDispatcher : public IObLogFetcherDispatcher
{
  static const int64_t DATA_OP_TIMEOUT = 10 * _SEC_;

public:
  ObLogMetaDataFetcherDispatcher();
  virtual ~ObLogMetaDataFetcherDispatcher();

  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag);

public:
  int init(
      IObLogMetaDataReplayer *log_meta_data_replayer,
      const int64_t start_seq);
  void destroy();

private:
  int dispatch_to_log_meta_data_replayer_(PartTransTask &task, volatile bool &stop_flag);

private:
  bool is_inited_;
  IObLogMetaDataReplayer *log_meta_data_replayer_;

  // DML and Global HeartBeat checkpoint seq
  // DDL global checkpoint seq:
  // 1. DDL trans
  // 2. DDL HeartBeat
  // 3. DDL Offline Task
  int64_t           checkpoint_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataFetcherDispatcher);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
