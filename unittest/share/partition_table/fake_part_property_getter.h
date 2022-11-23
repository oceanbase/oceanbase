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

#ifndef OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_
#define OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_

#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{

namespace host
{
const static common::ObAddr A = common::ObAddr(common::ObAddr::IPV4, "8.8.8.1", 80);
const static common::ObAddr B = common::ObAddr(common::ObAddr::IPV4, "8.8.8.2", 80);
const static common::ObAddr C = common::ObAddr(common::ObAddr::IPV4, "8.8.8.3", 80);
const static common::ObAddr D = common::ObAddr(common::ObAddr::IPV4, "8.8.8.4", 80);
const static common::ObAddr E = common::ObAddr(common::ObAddr::IPV4, "8.8.8.5", 80);
const static common::ObAddr F = common::ObAddr(common::ObAddr::IPV4, "8.8.8.6", 80);
const static common::ObAddr G = common::ObAddr(common::ObAddr::IPV4, "8.8.8.7", 80);
const static common::ObAddr H = common::ObAddr(common::ObAddr::IPV4, "8.8.8.8", 80);

const static common::ObAddr A1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.1", 80);
const static common::ObAddr B1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.2", 80);
const static common::ObAddr C1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.3", 80);
const static common::ObAddr D1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.4", 80);
const static common::ObAddr E1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.5", 80);

const static common::ObAddr A2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.1", 80);
const static common::ObAddr B2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.2", 80);
const static common::ObAddr C2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.3", 80);
const static common::ObAddr D2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.4", 80);
const static common::ObAddr E2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.5", 80);

const static common::ObAddr A3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.1", 80);
const static common::ObAddr B3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.2", 80);
const static common::ObAddr C3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.3", 80);
const static common::ObAddr D3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.4", 80);
const static common::ObAddr E3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.5", 80);

const static common::ObAddr A4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.1", 80);
const static common::ObAddr B4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.2", 80);
const static common::ObAddr C4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.3", 80);
const static common::ObAddr D4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.4", 80);
const static common::ObAddr E4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.5", 80);

const static common::ObZone ZONE1("zone1");
const static common::ObZone ZONE2("zone2");
const static common::ObZone ZONE3("zone3");
const static common::ObZone ZONE4("zone4");
const static common::ObZone ZONE5("zone5");
const static common::ObZone ZONE6("zone6");

const static common::ObRegion REGION1("region1");
const static common::ObRegion REGION2("region2");
const static common::ObRegion REGION3("region3");

} // end nameapce host

// implement ObIPartPropertyGetter for unittesting.
// all inlined for convenience.
class FakePartPropertyGetter
{
public:
  static uint64_t &TEN() { static uint64_t id = 1; return id; }
  static uint64_t &TID() { static uint64_t tid = 1; return tid; }
  static int64_t &PID() { static int64_t pid = 0; return pid; }
  static int64_t &PCNT() { static int64_t p_cnt = 0; return p_cnt; }
  static uint64_t &UID() { static uint64_t uid = common::OB_INVALID_ID; return uid; }
  const static int64_t DATA_VERSION = 1;
  const static common::ObZone ZONE;

  FakePartPropertyGetter() {};
  virtual ~FakePartPropertyGetter() {};

};
const common::ObZone FakePartPropertyGetter::ZONE = "1";

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_
