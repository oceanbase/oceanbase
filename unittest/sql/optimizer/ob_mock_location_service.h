/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_MOCK_LOCATION_SERVICE_H_
#define OB_MOCK_LOCATION_SERVICE_H_
#include "share/location_cache/ob_location_service.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace share
{

class MockLocationService : public ObLocationService
{
public:
  MockLocationService() : local_addr_(), sql_port_(0) {}
  virtual ~MockLocationService() {}
  int init(const common::ObAddr &local_addr, const int64_t sql_port);
  virtual int nonblock_get(const uint64_t tenant_id,
                           const ObTabletID &tablet_id,
                           ObLSID &ls_id) override;
  virtual int nonblock_get(const int64_t cluster_id,
                           const uint64_t tenant_id,
                           const ObLSID &ls_id,
                           ObLSLocation &location) override;
private:
    ObAddr local_addr_;
    int64_t sql_port_;
};

} // share
} // oceanbase

#endif /* OB_MOCK_LOCATION_SERVICE_H_ */
