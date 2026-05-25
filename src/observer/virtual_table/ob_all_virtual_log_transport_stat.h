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

#ifndef OB_ALL_VIRTUAL_LOG_TRANSPORT_STAT_H_
#define OB_ALL_VIRTUAL_LOG_TRANSPORT_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "logservice/transportservice/ob_log_transport_service.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualLogTransportStat : public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualLogTransportStat(omt::ObMultiTenant *omt) : omt_(omt) {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  class IterTransportStatusFunctor
  {
  public:
    explicit IterTransportStatusFunctor(ObAllVirtualLogTransportStat &host) : host_(host) {}
    int operator()(const logservice::LogTransportStatus &transport_status);
  private:
    ObAllVirtualLogTransportStat &host_;
  };
  class IterTenantFunctor
  {
  public:
    explicit IterTenantFunctor(IterTransportStatusFunctor &iter_functor) : iter_functor_(iter_functor) {}
    int operator()();
  private:
    IterTransportStatusFunctor &iter_functor_;
  };
private:
  int insert_stat_(logservice::LogTransportStat &transport_stat);
private:
  static const int64_t VARCHAR_32 = 32;
  char role_str_[VARCHAR_32] = {'\0'};
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char standby_addr_str_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  omt::ObMultiTenant *omt_;
};
} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_LOG_TRANSPORT_STAT_H_
