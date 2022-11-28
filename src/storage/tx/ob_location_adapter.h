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

#ifndef OCEANBASE_TRANSACTION_OB_LOCATION_ADAPTER_H_
#define OCEANBASE_TRANSACTION_OB_LOCATION_ADAPTER_H_

#include <stdint.h>
#include "ob_trans_define.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"

#include "common/ob_clock_generator.h"

namespace oceanbase
{

namespace common
{
class ObAddr;
}

namespace share
{
class ObLSID;
class ObLSLocation;
class ObLocationService;
}

namespace transaction
{

class ObILocationAdapter
{
public:
  ObILocationAdapter() {}
  virtual ~ObILocationAdapter() {}
  virtual int init(share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLocationService *location_service) = 0;
  virtual void destroy() = 0;
public:
  virtual int nonblock_get_leader(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id,
      common::ObAddr &leader) = 0;
  virtual int nonblock_renew(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id) = 0;
  virtual int nonblock_get(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id,
      share::ObLSLocation &location) = 0;
};

class ObLocationAdapter : public ObILocationAdapter
{
public:
  ObLocationAdapter();
  ~ObLocationAdapter() {}

  int init(share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLocationService *location_service);
  void destroy();
public:
  int nonblock_get_leader(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id,
      common::ObAddr &leader);
  int nonblock_renew(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id);
  int nonblock_get(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id,
      share::ObLSLocation &location);
private:
  int get_leader_(const int64_t cluster_id, const int64_t tenant_id, const share::ObLSID &ls_id,
      common::ObAddr &leader, const bool is_sync);
  void reset_statistics();
  void statistics();
private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLocationService *location_service_;
  // for statistics
  int64_t renew_access_;
  int64_t total_access_;
  int64_t error_count_;
};

} // transaction
} // oceanbase

#endif
