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

#include "storage/ls/ob_ls_meta_package.h"

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObLSMetaPackage,
                    ls_meta_,
                    palf_meta_);

ObLSMetaPackage::ObLSMetaPackage()
  : ls_meta_(),
    palf_meta_()
{
}

ObLSMetaPackage::ObLSMetaPackage(const ObLSMetaPackage &other)
  : ls_meta_(other.ls_meta_),
    palf_meta_(other.palf_meta_)
{
}

ObLSMetaPackage &ObLSMetaPackage::operator=(const ObLSMetaPackage &other)
{
  if (this != &other) {
    ls_meta_ = other.ls_meta_;
    palf_meta_ = other.palf_meta_;
  }
  return *this;
}

void ObLSMetaPackage::reset()
{
  ls_meta_.reset();
  palf_meta_.reset();
}

bool ObLSMetaPackage::is_valid() const
{
  return (ls_meta_.is_valid() &&
          palf_meta_.is_valid());
}

}
}
