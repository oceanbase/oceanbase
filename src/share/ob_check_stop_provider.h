/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SHARE_CHECK_STOP_PROVIDER_H__
#define __OB_SHARE_CHECK_STOP_PROVIDER_H__
namespace oceanbase
{
namespace share
{
class ObCheckStopProvider
{
public:
  virtual ~ObCheckStopProvider() {}
  // return OB_CANCELED if stop, else return OB_SUCCESS
  virtual int check_stop() const = 0;
};
}
}
#endif /* __OB_SHARE_CHECK_STOP_PROVIDER_H__ */

