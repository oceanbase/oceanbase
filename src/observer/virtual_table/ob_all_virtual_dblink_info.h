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

#ifndef OB_ALL_VIRTUAL_DBLINK_INFO_H_
#define OB_ALL_VIRTUAL_DBLINK_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
#ifndef OB_BUILD_DBLINK
namespace common {
  struct Dblink_status {
  uint64_t link_id;
  uint64_t link_tenant_id;
  int64_t protocol;
  uint64_t heterogeneous;
  uint64_t conn_opened;
  uint64_t conn_closed;
  uint64_t stmt_executed;
  uint16_t charset_id;
  uint16_t ncharset_id;
  ObString extra_info;
  void reset() {
    link_id = 0;
    link_tenant_id = 0;
    protocol = 0;
    heterogeneous = 0;
    conn_opened = 0;
    conn_closed = 0;
    stmt_executed = 0;
    charset_id = 0;
    ncharset_id = 0;
    extra_info.reset();
  }
  Dblink_status() {
    reset();
  }
  TO_STRING_KV(K(link_id),
               K(link_tenant_id),
               K(protocol),
               K(heterogeneous),
               K(conn_opened),
               K(conn_closed),
               K(stmt_executed),
               K(charset_id),
               K(ncharset_id),
               K(extra_info));
};
}
#endif

namespace observer
{
class ObAllVirtualDblinkInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDblinkInfo();
  virtual ~ObAllVirtualDblinkInfo();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  virtual int inner_open();
  inline void set_tenant_id(uint64_t tid) { tenant_id_ = tid; }
  int set_addr(const common::ObAddr &addr);
  int fill_cells(ObNewRow *&row, oceanbase::common::Dblink_status &dlink_status);
  inline ObIAllocator *get_allocator() { return allocator_; }
private:
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    LINK_ID,
    LOGGED_ON,
    HETEROGENEOUS,
    PROTOCOL,
    OPEN_CURSORS,
    IN_TRANSACTION,
    UPDATE_SENT,
    COMMIT_POINT_STRENGTH,
    LINK_TENANT_ID,
    OCI_CONN_OPENED,
    OCI_CONN_CLOSED,
    OCI_STMT_EXECUTED,
    OCI_ENV_CHARSET,
    OCI_ENV_NCHARSET,
    EXTRA_INFO,
  };
  uint64_t tenant_id_;
  common::ObString ipstr_;
  uint32_t port_;
  uint64_t row_cnt_;
  ObArray<common::Dblink_status> link_status_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDblinkInfo);
};
} // namespace observer
} // namespace oceanbase


#endif