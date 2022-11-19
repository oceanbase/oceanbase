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

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_REPLAY_STAT_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_REPLAY_STAT_H_

#include "common/row/ob_row.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "logservice/replayservice/ob_replay_status.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualReplayStat : public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualReplayStat(omt::ObMultiTenant *omt) : omt_(omt) {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int insert_stat_(logservice::LSReplayStat &replay_stat);
private:
  static const int64_t VARCHAR_32 = 32;
  char role_str_[VARCHAR_32] = {'\0'};
  char ip_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  omt::ObMultiTenant *omt_;
};
} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_REPLAY_STAT_H_ */