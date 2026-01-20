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

#ifndef OB_ALL_VIRTUAL_CATALOG_CLIENT_POOL_STAT_H_
#define OB_ALL_VIRTUAL_CATALOG_CLIENT_POOL_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"

namespace oceanbase
{
namespace share
{
  struct ObCatalogClientPoolKey;
  template <typename T> class ObCatalogClientPool;  // Forward declaration
  class ObCurlRestClient;
  class ObHiveMetastoreClient;
}

namespace observer
{
using namespace oceanbase::share;
enum CLIENT_TYPE
{
  CLIENT_TYPE_ALL = 0,
  CLIENT_TYPE_HMS = 1,
  CLIENT_TYPE_REST = 2,
};
class ObExternalCatalogClientPoolGetter
{
public:
  explicit ObExternalCatalogClientPoolGetter(common::ObScanner &scanner,
                          common::ObIArray<uint64_t> &output_column_ids,
                          char *svr_ip,
                          int32_t port,
                          common::ObNewRow &cur_row,
                          uint64_t effective_tenant_id,
                          enum CLIENT_TYPE client_type)
      : scanner_(scanner), output_column_ids_(output_column_ids), svr_ip_(svr_ip), port_(port),
          cur_row_(cur_row), effective_tenant_id_(effective_tenant_id), client_type_(client_type)
  {
  }
  virtual ~ObExternalCatalogClientPoolGetter() {};
  template <typename T>
  int operator() (common::hash::HashMapPair<ObCatalogClientPoolKey, share::ObCatalogClientPool<T>*> &entry)
  {
    int ret = OB_SUCCESS;
    ObObj *cells = cur_row_.cells_;
    share::ObCatalogClientPool<T> *client_pool = entry.second;
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
        case common::OB_APP_MIN_COLUMN_ID: { // CLIENT_TYPE
          if (client_type_ == CLIENT_TYPE_REST) {
            cells[cell_idx].set_varchar("rest");
          } else if (client_type_ == CLIENT_TYPE_HMS) {
            cells[cell_idx].set_varchar("hms");
          } else {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "invalid client type", K(ret), K(client_type_));
          }
          if (OB_SUCC(ret)) {
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 1: { // SVR_IP
          cells[cell_idx].set_varchar(svr_ip_);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 2: { // SVR_PORT
          cells[cell_idx].set_int(port_);
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 3: { // TENANT_ID
          cells[cell_idx].set_int(client_pool->get_tenant_id());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 4: { // CATALOG_ID
          cells[cell_idx].set_int(client_pool->get_catalog_id());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 5: { // URI
          cells[cell_idx].set_varchar(client_pool->get_uri().ptr());
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 6: { // TOTAL_CLIENTS
          cells[cell_idx].set_int(client_pool->get_size());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 7: { // IN_USE_CLIENTS
          cells[cell_idx].set_int(client_pool->get_in_use_clients_cnt());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 8: { // IDLE_CLIENTS
          cells[cell_idx].set_int(client_pool->get_idle_clients_cnt());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 9: { // WAITING_CLIENTS
          cells[cell_idx].set_int(client_pool->get_waiting_cnt());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 10: { // REF_CNT
          cells[cell_idx].set_int(client_pool->get_ref_cnt());
          break;
        }
        case common::OB_APP_MIN_COLUMN_ID + 11: { // LAST_ACCESS_TS
          cells[cell_idx].set_int(client_pool->get_last_access_ts());
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
          break;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
    }
    return ret;
  }
  void set_client_type(enum CLIENT_TYPE client_type) { client_type_ = client_type; }
  private:
  common::ObScanner &scanner_;
  common::ObIArray<uint64_t> &output_column_ids_;
  char *svr_ip_;
  int32_t port_;
  common::ObNewRow &cur_row_;
  uint64_t effective_tenant_id_;
  enum CLIENT_TYPE client_type_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalCatalogClientPoolGetter);
};

class ObAllVirtualExternalCatalogClientPoolStat : public common::ObVirtualTableScannerIterator
{
  friend class ObExternalCatalogClientPoolGetter;
public:
  ObAllVirtualExternalCatalogClientPoolStat();
  virtual ~ObAllVirtualExternalCatalogClientPoolStat();
  void destroy();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int inner_open() override;

private:
  int32_t port_;
  char svr_ip_[common::OB_IP_STR_BUFF];
  int fill_scanner(const uint64_t &tenant_id);
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t tenant_idx_;
  enum CLIENT_TYPE client_type_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualExternalCatalogClientPoolStat);
};

} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_CATALOG_CLIENT_POOL_STAT_H_