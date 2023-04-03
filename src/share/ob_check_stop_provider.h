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

