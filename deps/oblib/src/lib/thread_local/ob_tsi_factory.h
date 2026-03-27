/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
