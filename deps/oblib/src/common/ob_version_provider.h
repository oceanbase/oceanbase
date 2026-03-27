/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_VERSION_PROVIDER_H
#define OCEANBASE_COMMON_OB_VERSION_PROVIDER_H
namespace oceanbase
{
namespace common
{
class ObVersionProvider
{
public:
  ObVersionProvider() {};
  virtual ~ObVersionProvider() {};
  virtual common::ObVersion get_frozen_version() const = 0;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObVersionProvider);
private:
  // data members
};
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_VERSION_PROVIDER_H */
