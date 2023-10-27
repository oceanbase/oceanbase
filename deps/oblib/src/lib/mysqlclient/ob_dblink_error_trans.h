/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDBLINKERROR_H
#define OBDBLINKERROR_H
#include "lib/utility/ob_edit_distance.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

#define TRANSLATE_CLIENT_ERR(ret, errmsg)  \
  const int orginal_ret = ret;\
  bool is_oracle_err = lib::is_oracle_mode();\
  int translate_ret = OB_SUCCESS;\
  if (OB_SUCCESS == ret) {\
  } else if (OB_SUCCESS != (translate_ret = oceanbase::common::sqlclient::ObDblinkErrorTrans::\
      external_errno_to_ob_errno(is_oracle_err, orginal_ret, errmsg, ret))) {\
    LOG_WARN("failed to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  } else {\
    LOG_WARN("succ to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  }

#define TRANSLATE_CLIENT_ERR_2(ret, is_oracle_err, errmsg)  \
  const int orginal_ret = ret;\
  int translate_ret = OB_SUCCESS;\
  if (OB_SUCCESS == ret) {\
  } else if (OB_SUCCESS != (translate_ret = oceanbase::common::sqlclient::ObDblinkErrorTrans::\
      external_errno_to_ob_errno(is_oracle_err, orginal_ret, errmsg, ret))) {\
    LOG_WARN("failed to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  } else {\
    LOG_WARN("succ to translate client error code", K(translate_ret), K(orginal_ret), K(ret), K(is_oracle_err), K(errmsg));\
  }

class ObDblinkErrorTrans {
public:
  static int external_errno_to_ob_errno(bool is_oci_client,
                                        int external_errno,
                                        const char *external_errmsg,
                                        int &ob_errno);
};

} // namespace sqlclient
} // namespace common
} // namespace oceanbase
#endif //OBDBLINKERROR_H