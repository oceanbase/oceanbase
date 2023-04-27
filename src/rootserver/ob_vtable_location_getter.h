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

#ifndef OCEANBASE_ROOTSERVER_OB_VTABLE_LOCATION_GETTER_H_
#define OCEANBASE_ROOTSERVER_OB_VTABLE_LOCATION_GETTER_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/location_cache/ob_vtable_location_service.h" // share::ObVtableLocationType

namespace oceanbase
{
namespace rootserver
{
class ObUnitManager;

class ObVTableLocationGetter
{
public:
  ObVTableLocationGetter(ObUnitManager &unit_mgr);
  virtual ~ObVTableLocationGetter();
  int get(const share::ObVtableLocationType &vtable_type,
          common::ObSArray<common::ObAddr> &servers);

private:
  int get_only_rs_vtable_location_(const share::ObVtableLocationType &vtable_type,
                                   common::ObSArray<common::ObAddr> &servers);
  int get_global_vtable_location_(const share::ObVtableLocationType &vtable_type,
                                  common::ObSArray<common::ObAddr> &servers);
  int get_tenant_vtable_location_(const share::ObVtableLocationType &vtable_type,
                                  common::ObSArray<common::ObAddr> &servers);

  ObUnitManager &unit_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVTableLocationGetter);
};

}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_VTABLE_LOCATION_GETTER_H_
