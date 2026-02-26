/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/macro_cache/ob_ext_table_disk_cache_common_meta.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

/*-----------------------------------------ObExtFileVersion-----------------------------------------*/
ObExtFileVersion::ObExtFileVersion() : content_digest_(), modify_time_(-1)
{
}

ObExtFileVersion::~ObExtFileVersion()
{
  reset();
}

ObExtFileVersion::ObExtFileVersion(const ObString &content_digest, const int64_t modify_time)
    : content_digest_(content_digest), modify_time_(modify_time > 0 ? modify_time : -1)
{
}

void ObExtFileVersion::reset()
{
  content_digest_.reset();
  modify_time_ = -1;
}

bool ObExtFileVersion::is_valid() const
{
  return !content_digest_.empty() || modify_time_ > 0;
}

bool ObExtFileVersion::operator==(const ObExtFileVersion &other) const
{
  return content_digest_ == other.content_digest_ && modify_time_ == other.modify_time_;
}

uint64_t ObExtFileVersion::hash() const
{
  uint64_t hash_value = 0;
  hash_value = content_digest_.hash();
  hash_value = murmurhash(&modify_time_, sizeof(modify_time_), hash_value);
  return hash_value;
}

} // namespace storage
} // namespace oceanbase