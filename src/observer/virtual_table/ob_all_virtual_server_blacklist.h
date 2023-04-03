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

#ifndef OCEANBASE_OB_ALL_VIRTUAL_SERVER_BLACKLIST_H_
#define OCEANBASE_OB_ALL_VIRTUAL_SERVER_BLACKLIST_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_server_blacklist.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualServerBlacklist : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualServerBlacklist();
  virtual ~ObAllVirtualServerBlacklist();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {self_addr_ = addr;}
private:
  int prepare_to_read_();
private:
  enum TBL_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DST_IP,
    DST_PORT,
    IS_IN_BLACKLIST
  };
private:
  bool ready_to_read_;
  common::ObAddr self_addr_;
  char self_ip_buf_[common::OB_IP_STR_BUFF];
  char dst_ip_buf_[common::OB_IP_STR_BUFF];
  share::ObServerBlacklist::ObBlacklistInfoIterator info_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerBlacklist);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OB_ALL_VIRTUAL_SERVER_BLACKLIST_H_
