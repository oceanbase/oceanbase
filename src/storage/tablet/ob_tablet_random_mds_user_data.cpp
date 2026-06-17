/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_tablet_random_mds_user_data.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_unify_serialize.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
bool ObTabletRandomMdsUserData::is_valid() const
{
  return true;
}

int ObTabletRandomMdsUserData::init_random_size(const int64_t auto_random_size)
{
  int ret = OB_SUCCESS;
  auto_random_size_ = auto_random_size;
  is_active_ = true;
  return ret;
}

int ObTabletRandomMdsUserData::assign(const ObTabletRandomMdsUserData &other)
{
  int ret = OB_SUCCESS;
  auto_random_size_ = other.auto_random_size_;
  is_active_ = other.is_active_;
  return ret;
}

void ObTabletRandomMdsUserData::reset()
{
  auto_random_size_ = OB_INVALID_SIZE;
  is_active_ = false;
}

int ObTabletRandomMdsUserData::get_random_part_data(int64_t &auto_random_size, bool &is_active) const
{
  int ret = OB_SUCCESS;
  auto_random_size = auto_random_size_;
  is_active = is_active_;
  return ret;
}

int ObTabletRandomMdsUserData::set_random_size(const int64_t auto_random_size)
{
  int ret = OB_SUCCESS;
  auto_random_size_ = auto_random_size;
  return ret;
}

int ObTabletRandomMdsUserData::set_is_active(const bool is_active)
{
  int ret = OB_SUCCESS;
  is_active_ = is_active;
  return ret;
}


OB_DEF_SERIALIZE(ObTabletRandomMdsUserData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, auto_random_size_, is_active_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletRandomMdsUserData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, auto_random_size_, is_active_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletRandomMdsUserData)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, auto_random_size_, is_active_);
  return len;
}

} // namespace storage
} // namespace oceanbase
