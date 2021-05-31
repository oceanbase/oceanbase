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

#include "share/ob_errno.h"
#include "ob_mask_set.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_se_array_iterator.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {

int ObMaskSet::get_not_mask(ObPartitionArray& partitions) const
{
  int ret = OB_SUCCESS;
  const int64_t size = partitions_.count();

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    ret = OB_NOT_INIT;
  } else {
    partitions.reset();
    for (int64_t i = 0; i < size && OB_SUCCESS == ret; i++) {
      if (!bitset_.has_member(i)) {
        if (OB_FAIL(partitions.push_back(partitions_[i]))) {
          TRANS_LOG(WARN, "push back error", KR(ret), "partition", partitions_[i]);
        }
      }
    }
  }

  return ret;
}

int ObMaskSet::get_mask(ObPartitionArray& partitions) const
{
  int ret = OB_SUCCESS;
  const int64_t size = partitions_.count();

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    ret = OB_NOT_INIT;
  } else {
    partitions.reset();
    for (int64_t i = 0; i < size && OB_SUCCESS == ret; i++) {
      if (bitset_.has_member(i)) {
        if (OB_FAIL(partitions.push_back(partitions_[i]))) {
          TRANS_LOG(WARN, "push back error", KR(ret), "partition", partitions_[i]);
        }
      }
    }
  }

  return ret;
}

bool ObMaskSet::is_all_mask() const
{
  bool bool_ret = true;
  const int64_t size = is_bounded_staleness_read_ ? pla_.count() : partitions_.count();

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    bool_ret = false;
  } else {
    // Determine whether the bitset is all marked
    bool_ret = (size == bitset_.num_members());
  }

  return bool_ret;
}

bool ObMaskSet::is_mask(const ObPartitionKey& partition)
{
  bool bool_ret = false;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    bool_ret = false;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
  } else {
    int64_t pos = 0;
    for (ObPartitionArray::iterator it = partitions_.begin(); it != partitions_.end() && false == bool_ret;
         it++, pos++) {
      if (*it == partition && bitset_.has_member(pos)) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

int ObMaskSet::mask(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    bool hit = false;
    for (ObPartitionArray::iterator it = partitions_.begin(); it != partitions_.end() && OB_SUCCESS == ret;
         it++, pos++) {
      if (*it == partition) {
        hit = true;
        if (OB_FAIL(bitset_.add_member(pos))) {
          TRANS_LOG(WARN, "bitset add member error", K(partition), K(pos), KR(ret));
        }
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (!hit) {
        ret = OB_MASK_SET_NO_NODE;
      }
    }
  }

  return ret;
}

int ObMaskSet::multi_mask(const ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); i++) {
    if (OB_FAIL(mask(partitions.at(i)))) {
      TRANS_LOG(WARN, "mask error", K(ret), K(partitions.at(i)));
    }
  }

  return ret;
}

void ObMaskSet::clear_set()
{
  bitset_.reuse();
}

void ObMaskSet::reset()
{
  partitions_.reset();
  pla_.reset();
  bitset_.reuse();
  is_inited_ = false;
  is_bounded_staleness_read_ = false;
}

int ObMaskSet::init(const ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;
  ObPartitionArray* parray = const_cast<ObPartitionArray*>(&partitions);

  if (is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet inited twice");
    ret = OB_INIT_TWICE;
  } else if (partitions.count() <= 0) {
    TRANS_LOG(WARN, "invalid argument", "partition_count", partitions.count());
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (ObPartitionArray::iterator it = parray->begin(); it != parray->end() && OB_SUCCESS == ret; it++) {
      if (!(*it).is_valid()) {
        TRANS_LOG(WARN, "partition is invalid", "partition", *it);
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(bitset_.reserve(partitions.count()))) {
        TRANS_LOG(WARN, "reserve bit set error", KR(ret), "count", partitions.count());
      } else if (OB_FAIL(partitions_.assign(partitions))) {
        TRANS_LOG(WARN, "partition assign error", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObMaskSet::get_not_mask(ObPartitionLeaderArray& pla) const
{
  int ret = OB_SUCCESS;
  const int64_t size = pla_.count();

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    ret = OB_NOT_INIT;
  } else {
    pla.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (!bitset_.has_member(i)) {
        if (OB_FAIL(pla.push(pla_.get_partitions().at(i), pla_.get_leaders().at(i)))) {
          TRANS_LOG(WARN,
              "partition leader push error",
              KR(ret),
              "partition",
              pla_.get_partitions().at(i),
              "addr",
              pla_.get_leaders().at(i));
        }
      }
    }
  }

  return ret;
}

bool ObMaskSet::is_mask(const ObPartitionKey& partition, const ObAddr& addr)
{
  bool bool_ret = false;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    bool_ret = false;
  } else if (!partition.is_valid() || !addr.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(addr));
  } else {
    for (int64_t i = 0; i < pla_.count(); ++i) {
      if (partition == pla_.get_partitions().at(i) && addr == pla_.get_leaders().at(i) && bitset_.has_member(i)) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

int ObMaskSet::mask(const ObPartitionKey& partition, const ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid() || !addr.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool hit = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pla_.count(); ++i) {
      if (partition == pla_.get_partitions().at(i) && addr == pla_.get_leaders().at(i)) {
        hit = true;
        if (OB_FAIL(bitset_.add_member(i))) {
          TRANS_LOG(WARN, "bitset add member error", K(partition), "pos", i, KR(ret));
        }
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (!hit) {
        ret = OB_MASK_SET_NO_NODE;
      }
    }
  }

  return ret;
}

int ObMaskSet::init(const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  const ObPartitionArray& partitions = pla.get_partitions();
  const ObAddrArray& addrs = pla.get_leaders();

  if (is_inited_) {
    TRANS_LOG(WARN, "ObMaskSet inited twice");
    ret = OB_INIT_TWICE;
  } else if (pla.count() <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(pla));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(bitset_.reserve(pla.count()))) {
    TRANS_LOG(WARN, "reserve bit set error", KR(ret), "count", pla.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
      if (!partitions.at(i).is_valid() || !addrs.at(i).is_valid()) {
        TRANS_LOG(WARN, "partition or addr is invalid", "partition", partitions.at(i), "addr", addrs.at(i));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(pla_.push(partitions.at(i), addrs.at(i)))) {
        TRANS_LOG(WARN, "partition leader push error", KR(ret));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      is_bounded_staleness_read_ = true;
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
