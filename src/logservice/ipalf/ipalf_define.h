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

#ifndef OCEANBASE_LOGSERVICE_IPALF_LOG_DEGINE_
#define OCEANBASE_LOGSERVICE_IPALF_LOG_DEGINE_

#include <stdint.h>

#include "lib/ob_errno.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"


namespace oceanbase
{
namespace ipalf
{
inline bool is_valid_palf_id(const int64_t id)
{
  return 0 <= id;
}

struct LSKey {
  LSKey() : id_(-1) {}
  explicit LSKey(const int64_t id) : id_(id) {}
  ~LSKey() {id_ = -1;}
  LSKey(const LSKey &key) { this->id_ = key.id_; }
  LSKey &operator=(const LSKey &other)
  {
    this->id_ = other.id_;
    return *this;
  }

  bool operator==(const LSKey &palf_id) const
  {
    return this->compare(palf_id) == 0;
  }
  bool operator!=(const LSKey &palf_id) const
  {
    return this->compare(palf_id) != 0;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&hash_val, sizeof(id_), id_);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const LSKey &palf_id) const
  {
    if (palf_id.id_ < id_) {
      return 1;
    } else if (palf_id.id_ == id_) {
      return 0;
    } else {
      return -1;
    }
  }
  void reset() {id_ = -1;}
  bool is_valid() const {return -1 != id_;}
  int64_t id_;
  TO_STRING_KV(K_(id));
};
} // end namespace ipalf
} // end namespace oceanbase

#endif