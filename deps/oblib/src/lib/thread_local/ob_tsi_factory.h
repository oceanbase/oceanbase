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

#ifndef  OCEANBASE_COMMON_OB_TSI_FACTORY_
#define  OCEANBASE_COMMON_OB_TSI_FACTORY_

#include "lib/stat/ob_di_tls.h"

namespace oceanbase
{
namespace common
{
#define GET_TSI0(type) (::oceanbase::common::ObDITls<type,0>::get_instance())
#define GET_TSI_MULT0(type, tag) (::oceanbase::common::ObDITls<type,tag>::get_instance())
#define GET_TSI(type) (::oceanbase::common::ObDITls<type,0>::get_instance())
#define GET_TSI_MULT(type, tag) (::oceanbase::common::ObDITls<type,tag>::get_instance())
} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_COMMON_TSI_FACTORY_
