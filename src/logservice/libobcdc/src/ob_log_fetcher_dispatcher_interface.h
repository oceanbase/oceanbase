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
 * The interface definition of Fetcher Data Dispatcher
 * After the data is generated (PartTransTask) in Fetcher, it is distributed through the data distributor
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER_INTERFACE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DISPATCHER_INTERFACE_H_

namespace oceanbase
{
namespace libobcdc
{
enum FetcherDispatcherType
{
  UNKNOWN,
  DATA_DICT_DIS_TYPE,
  CDC_DIS_TYPE
};

class PartTransTask;
class IObLogFetcherDispatcher
{
public:
  IObLogFetcherDispatcher(FetcherDispatcherType dispatch_type) : dispatch_type_(dispatch_type) {}
  virtual ~IObLogFetcherDispatcher() {}

  bool is_data_dict_dispatcher() const { return FetcherDispatcherType::DATA_DICT_DIS_TYPE == dispatch_type_; }
  bool is_cdc_dispatcher() const { return FetcherDispatcherType::CDC_DIS_TYPE == dispatch_type_; }

  // DDL/DML: Support for dispatch all kinds of partition transaction tasks
  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag) = 0;

  FetcherDispatcherType dispatch_type_;
};

}
}

#endif
