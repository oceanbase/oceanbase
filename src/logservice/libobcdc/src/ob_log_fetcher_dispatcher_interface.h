/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
