/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
private:
  bool need_record_;
  bool prev_stat_;
};

}
}
#endif /* _OB_SHARE_ASH_BKGD_SESS_INACTIVE_GUARD_H_ */
//// end of header file
