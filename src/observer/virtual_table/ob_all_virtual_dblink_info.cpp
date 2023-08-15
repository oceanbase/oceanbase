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

#include "ob_all_virtual_dblink_info.h"
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oci_environment.h"
#include "lib/oracleclient/ob_oracle_oci_connection.h"
#endif
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{
ObAllVirtualDblinkInfo::ObAllVirtualDblinkInfo()
  : ObVirtualTableScannerIterator(),
  tenant_id_(OB_INVALID_ID),
  ipstr_(),
  port_(0),
  row_cnt_(0),
  link_status_()
{
}

ObAllVirtualDblinkInfo::~ObAllVirtualDblinkInfo()
{
  reset();
}

void ObAllVirtualDblinkInfo::reset()
{
  ObVirtualTableScannerIterator::reset();
  tenant_id_ = OB_INVALID_ID;
  ipstr_.reset();
  port_ = 0;
  row_cnt_ = 0;
  link_status_.reset();
}

int ObAllVirtualDblinkInfo::set_addr(const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString ipstr = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

int ObAllVirtualDblinkInfo::inner_open()
{
  int ret = OB_SUCCESS;
  ObDbLinkProxy *proxy = GCTX.dblink_proxy_;
  common::sqlclient::ObDbLinkConnectionPool *link_pool = NULL;
  if (OB_ISNULL(proxy)) {
    // do nothing
  } else if (OB_ISNULL(get_allocator())) {
    // do nothing
  } else if (OB_ISNULL(link_pool = proxy->get_dblink_conn_pool())) {
    // do nothing
#ifdef OB_BUILD_DBLINK
  } else if (OB_FAIL(link_pool->get_oci_pool().get_dblink_status(link_status_, *get_allocator()))) {
    SERVER_LOG(ERROR, "failed to get link status", K(ret));
#endif
  } else {
    row_cnt_ = 0;
  }
  return ret;
}

int ObAllVirtualDblinkInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (row_cnt_ < link_status_.count()) {
    if (OB_FAIL(fill_cells(row, link_status_.at(row_cnt_)))) {
      SERVER_LOG(ERROR, "failed to fill cells", K(ret), K(row_cnt_));
    } else {
      row_cnt_++;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualDblinkInfo::fill_cells(ObNewRow *&row, common::Dblink_status &dlink_status)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  const int64_t col_count = output_column_ids_.count();
  ObCharsetType default_charset = ObCharset::get_default_charset();
  ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
  if (OB_UNLIKELY(NULL == cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(tenant_id_);
          break;
        }
        case SVR_IP: {
          cells[i].set_varchar(ipstr_);
          cells[i].set_collation_type(default_collation);
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(port_);
          break;
        }
        case LINK_ID: {
          cells[i].set_int(dlink_status.link_id);
          break;
        }
        case LOGGED_ON: {
          cells[i].set_int(1);
          break;
        }
        case HETEROGENEOUS: {
          cells[i].set_int(dlink_status.heterogeneous);
          break;
        }
        case PROTOCOL: {
          cells[i].set_int(dlink_status.protocol);
          break;
        }
        case OPEN_CURSORS: {
          cells[i].set_int(0);
          break;
        }
        case IN_TRANSACTION: {
          cells[i].set_int(0);
          break;
        }
        case UPDATE_SENT: {
          cells[i].set_int(0);
          break;
        }
        case COMMIT_POINT_STRENGTH: {
          cells[i].set_int(0);
          break;
        }
        case LINK_TENANT_ID: {
          cells[i].set_int(dlink_status.link_tenant_id);
          break;
        }
        case OCI_CONN_OPENED: {
          cells[i].set_int(dlink_status.conn_opened);
          break;
        }
        case OCI_CONN_CLOSED: {
          cells[i].set_int(dlink_status.conn_closed);
          break;
        }
        case OCI_STMT_EXECUTED: {
          cells[i].set_int(dlink_status.stmt_executed);
          break;
        }
        case OCI_ENV_CHARSET: {
          cells[i].set_int(dlink_status.charset_id);
          break;
        }
        case OCI_ENV_NCHARSET: {
          cells[i].set_int(dlink_status.ncharset_id);
          break;
        }
        case EXTRA_INFO: {
          cells[i].set_varchar(dlink_status.extra_info);
          cells[i].set_collation_type(default_collation);
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "unexpected column id", K(col_id), K(ret));
          break;
      }
    }
    OX (row = &cur_row_);
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
