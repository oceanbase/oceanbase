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

#define USING_LOG_PREFIX STORAGE

#include "ob_tablet_table_store_flag.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

/* ObTabletTableStoreFlag */

ObTabletTableStoreFlag::ObTabletTableStoreFlag()
  : with_major_sstable_(ObTabletTableStoreWithMajorFlag::WITH_MAJOR_SSTABLE),
    reserved_()
{
}

ObTabletTableStoreFlag::~ObTabletTableStoreFlag()
{
}

void ObTabletTableStoreFlag::reset()
{
  with_major_sstable_ = ObTabletTableStoreWithMajorFlag::WITH_MAJOR_SSTABLE;
}

int ObTabletTableStoreFlag::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, status_))) {
    LOG_WARN("serialize ha status failed.", K(ret), K(new_pos), K(len), K_(status), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObTabletTableStoreFlag::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &status_))) {
    LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObTabletTableStoreFlag::get_serialize_size() const
{
  return serialization::encoded_length_i64(status_);
}

} // end namespace storage
} // end namespace oceanbase