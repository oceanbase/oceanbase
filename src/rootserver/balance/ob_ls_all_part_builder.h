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

#ifndef OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H
#define OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H

#include "share/transfer/ob_transfer_info.h"    // ObTransferPartList, ObTransferPartInfo

namespace oceanbase
{
namespace common
{
class ObISQLClient;
} // ns common

namespace share
{
class ObLSID;
class ObTabletToLSInfo;

namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
} // ns schema
} // ns share

namespace rootserver
{

class ObLSAllPartBuilder
{
public:
  // build current all part list on specific LS
  //
  // @return OB_NEED_RETRY     schema not latest or schema changed during handling
  static int build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObISQLClient &sql_proxy,
      share::ObTransferPartList &part_list);

private:
  static int get_latest_schema_guard_(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObISQLClient &sql_proxy,
      share::schema::ObSchemaGetterGuard &schema_guard);
  static int build_part_info_(
      const uint64_t tenant_id,
      share::ObTabletToLSInfo &tablet,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::ObTransferPartInfo &part_info,
      bool &need_skip);
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H */
