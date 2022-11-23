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

#ifndef OCEANBASE_PARTITION_TABLE_OB_ISERVER_TRACE_H_
#define OCEANBASE_PARTITION_TABLE_OB_ISERVER_TRACE_H_

#include "common/ob_zone.h"
#include "common/ob_region.h"
namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace share
{


// trace server alive status
class ObIServerTrace
{
public:
  ObIServerTrace() {}
  virtual ~ObIServerTrace() {};

  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const = 0;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const = 0;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &is_block) const = 0;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_alive) const = 0;
  virtual int is_server_exist(const common::ObAddr &server, bool &exist) const = 0;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const = 0;
};

class ObIZoneTrace
{
public:
  ObIZoneTrace() {}
  virtual ~ObIZoneTrace() {}
  virtual int get_region(const common::ObZone &zone, common::ObRegion &region) const = 0;
  virtual int get_zone_count(const common::ObRegion &region, int64_t &zone_count) const = 0;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_PARTITION_TABLE_OB_ISERVER_TRACE_H_
