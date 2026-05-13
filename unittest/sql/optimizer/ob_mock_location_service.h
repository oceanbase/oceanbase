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
