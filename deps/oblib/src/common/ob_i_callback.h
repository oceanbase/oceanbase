/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_I_CALLBAK_
#define OCEANBASE_COMMON_OB_I_CALLBAK_

namespace oceanbase
{
namespace common
{
class ObICallback
{
public:
  ObICallback() {}
  virtual ~ObICallback() {}
  virtual int callback() = 0;
};

class ObICallbackHandler
{
public:
  ObICallbackHandler() {}
  virtual ~ObICallbackHandler() {}
  virtual int handle_callback(ObICallback *task) = 0;
};
}; // end namespace common
}; // end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_I_CALLBAK_
