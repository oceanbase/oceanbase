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
