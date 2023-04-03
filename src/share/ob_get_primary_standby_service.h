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

#ifndef OCEANBASE_SHARE_OB_GET_PRIMARY_STANDBY_SERVICE_H_
#define OCEANBASE_SHARE_OB_GET_PRIMARY_STANDBY_SERVICE_H_

#include "share/ob_primary_standby_service.h"



#define OB_PRIMARY_STANDBY_SERVICE_TYPE ObPrimaryStandbyService

namespace oceanbase
{
namespace standby
{

class ObPrimaryStandbyServiceGetter
{
public:
  static ObPrimaryStandbyService &get_instance()
  {
    static OB_PRIMARY_STANDBY_SERVICE_TYPE primary_standby_service;
    return primary_standby_service;
  }
};

#define OB_PRIMARY_STANDBY_SERVICE (oceanbase::standby::ObPrimaryStandbyServiceGetter::get_instance())

}  // end namespace standby
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_GET_PRIMARY_STANDBY_SERVICE_H_
