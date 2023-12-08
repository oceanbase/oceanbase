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

#ifndef _OB_SHARE_ASH_BKGD_SESS_INACTIVE_GUARD_H_
#define _OB_SHARE_ASH_BKGD_SESS_INACTIVE_GUARD_H_

namespace oceanbase
{
namespace common
{

class ObBKGDSessInActiveGuard
{
public:
  ObBKGDSessInActiveGuard();
  ~ObBKGDSessInActiveGuard();
};

}
}
#endif /* _OB_SHARE_ASH_BKGD_SESS_INACTIVE_GUARD_H_ */
//// end of header file
