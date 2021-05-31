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

#include "lib/container/ob_id_set.h"

namespace oceanbase {
namespace common {

ObId2Idx::ObId2Idx()
{}

ObId2Idx::~ObId2Idx()
{}

void ObId2Idx::reset()
{
  id2idx_.reset();
}

int ObId2Idx::add_id(uint64_t id, int32_t* idx /*=NULL*/)
{
  int ret = OB_SUCCESS;
  int32_t new_idx = static_cast<int32_t>(id2idx_.count());
  if (OB_FAIL(id2idx_.set_refactored(id, new_idx, 0))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_ERR_ALREADY_EXISTS;
    }
  } else {
    if (NULL != idx) {
      *idx = new_idx;
    }
  }
  return ret;
}

bool ObId2Idx::has_id(uint64_t id) const
{
  return NULL != id2idx_.get(id);
}

int32_t ObId2Idx::get_idx(uint64_t id) const
{
  int32_t ret = -1;
  if (OB_SUCCESS != id2idx_.get_refactored(id, ret)) {
    ret = -1;
  }
  return ret;
}
////////////////////////////////////////////////////////////////
ObIdSet::ObIdSet()
{}

ObIdSet::~ObIdSet()
{}

void ObIdSet::reset()
{
  id_set_.reset();
  bit_set_.reset();
}

int ObIdSet::add_id(uint64_t id, int32_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == id) || OB_UNLIKELY(0 > idx) || OB_UNLIKELY(idx >= OB_MAX_TABLE_NUM_PER_STMT)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(id_set_.set_refactored(id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ERR_ALREADY_EXISTS;
      }
    } else if (OB_FAIL(bit_set_.add_member(idx))) {
      LIB_LOG(ERROR, "failed to set bit member", K(id), K(idx), K(ret));
    }
  }
  return ret;
}

bool ObIdSet::has_id(uint64_t id)
{
  return OB_HASH_EXIST == id_set_.exist_refactored(id);
}

bool ObIdSet::includes_id_set(const ObIdSet& other) const
{
  return other.bit_set_.is_subset(this->bit_set_);
}

bool ObIdSet::intersect_id_set(const ObIdSet& other) const
{
  return other.bit_set_.overlap(this->bit_set_);
}

}  // namespace common
}  // namespace oceanbase
