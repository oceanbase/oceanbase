/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_LS_LOCATION_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_LS_LOCATION_

#include "ob_all_virtual_proxy_base.h" // ObAllVirtualProxyBaseIterator
#include "lib/container/ob_se_array.h" // ObSEArray
#include "share/location_cache/ob_location_struct.h" // ObLSLocation

namespace oceanbase
{
namespace observer
{
class ObAllVirtualProxyLSLocation : public ObAllVirtualProxyBaseIterator
{
  enum ALL_VIRTUAL_PROXY_LS_LOCATION_TABLE_COLUMNS
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    LS_ID,
    SVR_IP,
    SQL_PORT,
    SVR_PORT,
    ROLE,
    REPLICA_TYPE,
    PROPOSAL_ID,
    SPARE1,
    SPARE2,
    SPARE3,
    SPARE4,
    SPARE5,
    SPARE6,
  };

  enum ALL_VIRTUAL_PROXY_LS_LOCATION_ROWKEY_IDX
  {
    TENANT_ID_IDX = 0,
    LS_ID_IDX,
    SVR_IP_IDX,
    SQL_PORT_IDX,
    ROW_KEY_COUNT,
  };

public:
  ObAllVirtualProxyLSLocation();
  virtual ~ObAllVirtualProxyLSLocation();

  virtual int inner_open();
  virtual int inner_get_next_row();

private:
  int parse_key_ranges_();
  int add_tenant_id_(const uint64_t tenant_id);
  int prepare_ls_locations_();
  int fill_row_(
      const share::ObLSLocation &location,
      const share::ObLSReplicaLocation &replica);

private:
  bool is_inited_;
  common::ObSEArray<uint64_t, 4> tenant_ids_;
  int64_t next_location_idx_;
  int64_t next_replica_idx_;
  common::ObSEArray<share::ObLSLocation, 4> ls_locations_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxyLSLocation);
};

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_LS_LOCATION_ */
