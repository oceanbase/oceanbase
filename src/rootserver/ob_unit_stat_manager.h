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

#ifndef OCEANBASE_ROOTSERVER_OB_UNIT_STAT__MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_UNIT_STAT__MANAGER_H_

#include "share/ob_unit_stat.h"
#include "rootserver/ob_root_utils.h"
#include "share/ob_unit_getter.h"
namespace oceanbase
{
namespace share
{
class ObUnitTableOperator;
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObUnitManager;
class ObUnitStatManager
{
public:
  ObUnitStatManager();
  virtual ~ObUnitStatManager() = default;

  virtual int init(common::ObMySQLProxy &sql_proxy);
  void reuse();
  virtual int gather_stat();
  virtual int get_unit_stat(
      uint64_t unit_id,
      const common::ObZone &zone,
      share::ObUnitStat &unit_stat);
private:
  bool inited_;
  bool loaded_stats_;
  share::ObUnitTableOperator ut_operator_;
  share::ObUnitStatMap unit_stat_map_;
  DISALLOW_COPY_AND_ASSIGN(ObUnitStatManager);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_UNIT_STAT__MANAGER_H_
