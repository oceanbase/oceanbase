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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}
}
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
}
namespace observer
{
class ObVirtualProxyServerStat : public common::ObVirtualTableProjector
{
public:
  ObVirtualProxyServerStat();
  virtual ~ObVirtualProxyServerStat();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy *sql_proxy,
           common::ObServerConfig *config);

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  struct ObServerStateInfo
  {
    ObServerStateInfo();
    ~ObServerStateInfo() {}
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(svr_ip_buf), K_(svr_ip_len), K_(svr_port),
                 K_(zone_name_buf), K_(zone_name_len),
                 K_(status_buf), K_(status_len),
                 K_(start_service_time), K_(stop_time));

    char svr_ip_buf_[common::MAX_IP_ADDR_LENGTH+1];
    int64_t svr_ip_len_;
    int64_t svr_port_;
    char zone_name_buf_[common::MAX_ZONE_LENGTH + 1];
    int64_t zone_name_len_;
    char status_buf_[common::OB_SERVER_STATUS_LENGTH+1];
    int64_t status_len_;
    int64_t start_service_time_;
    int64_t stop_time_;
  };

  int get_full_row(const share::schema::ObTableSchema *table,
                   const ObServerStateInfo &server_state,
                   common::ObIArray<Column> &columns);
  int get_next_server_state();
  int get_all_server_state();

  bool is_inited_;
  bool is_queried_;
  const share::schema::ObTableSchema *table_schema_;
  common::ObMySQLProxy* sql_proxy_;
  ObServerStateInfo server_state_;
  int64_t server_idx_;
  common::ObArray<ObServerStateInfo> server_states_;
  common::ObServerConfig *config_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualProxyServerStat);
};

}//end namespace observer
}//end namespace oceanbase
#endif  /*OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_*/
