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

#ifndef OCEANBASE_LOGSERVICE_OB_LOCATION_ADAPTER_H_
#define OCEANBASE_LOGSERVICE_OB_LOCATION_ADAPTER_H_

#include <stdint.h>
#include "logservice/palf/palf_callback.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace share
{
class ObLocationService;
}
namespace logservice
{
class ObLocationAdapter : public palf::PalfLocationCacheCb
{
public:
  ObLocationAdapter();
  virtual ~ObLocationAdapter();
  int init(share::ObLocationService *location_service);
  void destroy();
public:
  // Gets leader address of a log stream synchronously.
  int get_leader(const int64_t id, common::ObAddr &leader);
  // Nonblock way to get leader address of the log stream.
  int nonblock_get_leader(int64_t id, common::ObAddr &leader);
  int nonblock_renew_leader(int64_t id);
  int nonblock_get_leader(const uint64_t tenant_id, int64_t id, common::ObAddr &leader);
  int nonblock_renew_leader(const uint64_t tenant_id, int64_t id);
private:
  bool is_inited_;
  share::ObLocationService *location_service_;
};

} // logservice
} // oceanbase

#endif
