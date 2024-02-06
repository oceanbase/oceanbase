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
                    palf_meta_,
                    dup_ls_meta_);

ObLSMetaPackage::ObLSMetaPackage()
  : ls_meta_(),
    palf_meta_(),
    dup_ls_meta_()
{
}

ObLSMetaPackage::ObLSMetaPackage(const ObLSMetaPackage &other)
    : ls_meta_(other.ls_meta_), palf_meta_(other.palf_meta_)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dup_ls_meta_.copy(other.dup_ls_meta_))) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "copy dup ls meta failed", K(dup_ls_meta_),
                      K(other.dup_ls_meta_))
  } else if (!dup_ls_meta_.is_valid()) {
    dup_ls_meta_.ls_id_ = ls_meta_.ls_id_;
    DUP_TABLE_LOG_RET(INFO, OB_SUCCESS, "copy a old version dup ls meta without ls_id_", KPC(this),
                      K(other.dup_ls_meta_))
  }
}

ObLSMetaPackage &ObLSMetaPackage::operator=(const ObLSMetaPackage &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    ls_meta_ = other.ls_meta_;
    palf_meta_ = other.palf_meta_;
    if (OB_FAIL(dup_ls_meta_.copy(other.dup_ls_meta_))) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "copy dup ls meta failed", K(dup_ls_meta_),
                        K(other.dup_ls_meta_))
    } else if (!dup_ls_meta_.is_valid()) {
      dup_ls_meta_.ls_id_ = ls_meta_.ls_id_;
      DUP_TABLE_LOG_RET(INFO, OB_SUCCESS, "copy a old version dup ls meta without ls_id_",
                        KPC(this), K(other.dup_ls_meta_))
    }
  }
  return *this;
}

void ObLSMetaPackage::reset()
{
  ls_meta_.reset();
  palf_meta_.reset();
  dup_ls_meta_.reset();
}

bool ObLSMetaPackage::is_valid() const
{
  return (ls_meta_.is_valid() &&
          palf_meta_.is_valid() &&
          dup_ls_meta_.is_valid());
}

}
}
