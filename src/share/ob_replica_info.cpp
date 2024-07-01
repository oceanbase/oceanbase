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

#define USING_LOG_PREFIX SHARE
#include "ob_define.h"
#include "share/ob_replica_info.h"
#include "rootserver/ob_locality_util.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
using namespace rootserver;
using namespace common;
namespace share
{
int64_t BaseReplicaAttrSet::get_full_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < get_full_replica_attr_array().count(); ++i) {
    num += get_full_replica_attr_array().at(i).num_;
  }
  return num;
}

int64_t BaseReplicaAttrSet::get_logonly_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < get_logonly_replica_attr_array().count(); ++i) {
    num += get_logonly_replica_attr_array().at(i).num_;
  }
  return num;
}

int64_t BaseReplicaAttrSet::get_readonly_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < get_readonly_replica_attr_array().count(); ++i) {
    num += get_readonly_replica_attr_array().at(i).num_;
  }
  return num;
}

int64_t BaseReplicaAttrSet::get_encryption_logonly_replica_num() const
{
  int64_t num = 0;
  for (int64_t i = 0; i < get_encryption_logonly_replica_attr_array().count(); ++i) {
    num += get_encryption_logonly_replica_attr_array().at(i).num_;
  }
  return num;
}

int64_t BaseReplicaAttrSet::get_paxos_replica_num() const
{
  return get_full_replica_num()
         + get_logonly_replica_num()
         + get_encryption_logonly_replica_num();
}

int64_t BaseReplicaAttrSet::get_specific_replica_num() const
{
  int64_t specific_replica_num = 0;
  if (ObLocalityDistribution::ALL_SERVER_CNT != get_full_replica_num()) {
    specific_replica_num += get_full_replica_num();
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != get_logonly_replica_num()) {
    specific_replica_num += get_logonly_replica_num();
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != get_readonly_replica_num()) {
    specific_replica_num += get_readonly_replica_num();
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != get_encryption_logonly_replica_num()) {
    specific_replica_num += get_encryption_logonly_replica_num();
  }
  return specific_replica_num;
}

bool ObReplicaAttrSet::is_paxos_replica_match(const ObReplicaAttrSet &that) const
{
  bool equal = true;
  if (full_replica_attr_array_.count() != that.full_replica_attr_array_.count()
      || logonly_replica_attr_array_.count() != that.logonly_replica_attr_array_.count()
      || encryption_logonly_replica_attr_array_.count() != that.encryption_logonly_replica_attr_array_.count()) {
    equal = false;
  } else {
    for (int64_t i = 0; equal && i < full_replica_attr_array_.count(); ++i) {
      if (full_replica_attr_array_.at(i) != that.full_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < logonly_replica_attr_array_.count(); ++i) {
      if (logonly_replica_attr_array_.at(i) != that.logonly_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < encryption_logonly_replica_attr_array_.count(); ++i) {
      if (encryption_logonly_replica_attr_array_.at(i) != that.encryption_logonly_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
  }
  return equal;
}

bool ObReplicaAttrSet::operator==(const ObReplicaAttrSet &that) const
{
  bool equal = true;
  if (full_replica_attr_array_.count() != that.full_replica_attr_array_.count()
      || logonly_replica_attr_array_.count() != that.logonly_replica_attr_array_.count()
      || readonly_replica_attr_array_.count() != that.readonly_replica_attr_array_.count()
      || encryption_logonly_replica_attr_array_.count() != that.encryption_logonly_replica_attr_array_.count()) {
    equal = false;
  } else {
    for (int64_t i = 0; equal && i < full_replica_attr_array_.count(); ++i) {
      if (full_replica_attr_array_.at(i) != that.full_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < logonly_replica_attr_array_.count(); ++i) {
      if (logonly_replica_attr_array_.at(i) != that.logonly_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < readonly_replica_attr_array_.count(); ++i) {
      if (readonly_replica_attr_array_.at(i) != that.readonly_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < encryption_logonly_replica_attr_array_.count(); ++i) {
      if (encryption_logonly_replica_attr_array_.at(i) != that.encryption_logonly_replica_attr_array_.at(i)) {
        equal = false;
      }
    }
  }
  return equal;
}

bool ObReplicaAttrSet::operator!=(
     const ObReplicaAttrSet &that) const
{
  return (!(*this == that));
}

int ObReplicaAttrSet::assign(const BaseReplicaAttrSet &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(full_replica_attr_array_.assign(that.get_full_replica_attr_array()))) {
    LOG_WARN("fail to assign full replica attr", KR(ret));
  } else if (OB_FAIL(logonly_replica_attr_array_.assign(that.get_logonly_replica_attr_array()))) {
    LOG_WARN("fail to assign logonly replica attr", KR(ret));
  } else if (OB_FAIL(readonly_replica_attr_array_.assign(that.get_readonly_replica_attr_array()))) {
    LOG_WARN("fail to assign readonly replica attr", KR(ret));
  } else if (OB_FAIL(encryption_logonly_replica_attr_array_.assign(
          that.get_encryption_logonly_replica_attr_array()))) {
    LOG_WARN("fail to assign encryption logonly replica attr array", KR(ret));
  }
  return ret;
}

int ObReplicaAttrSet::set_replica_attr_array(
    const common::ObIArray<share::ReplicaAttr> &full_replica_attr_array,
    const common::ObIArray<share::ReplicaAttr> &logonly_replica_attr_array,
    const common::ObIArray<share::ReplicaAttr> &readonly_replica_attr_array,
    const common::ObIArray<share::ReplicaAttr> &encryption_logonly_replica_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(full_replica_attr_array_.assign(full_replica_attr_array))) {
    LOG_WARN("fail to assign full replica attr array", KR(ret));
  } else if (OB_FAIL(logonly_replica_attr_array_.assign(logonly_replica_attr_array))) {
    LOG_WARN("fail to assign logonly replica attr array", KR(ret));
  } else if (OB_FAIL(readonly_replica_attr_array_.assign(readonly_replica_attr_array))) {
    LOG_WARN("fail to assign readonly replica attr array", KR(ret));
  } else if (OB_FAIL(encryption_logonly_replica_attr_array_.assign(encryption_logonly_replica_attr_array))) {
    LOG_WARN("fail to assign encryption logonly replica attr array", KR(ret));
  }
  return ret;
}

int ObReplicaAttrSet::set_paxos_replica_attr_array(
    const common::ObIArray<share::ReplicaAttr> &full_replica_attr_array,
    const common::ObIArray<share::ReplicaAttr> &logonly_replica_attr_array,
    const common::ObIArray<share::ReplicaAttr> &encryption_logonly_replica_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(full_replica_attr_array_.assign(full_replica_attr_array))) {
    LOG_WARN("fail to assign full replica attr array", KR(ret));
  } else if (OB_FAIL(logonly_replica_attr_array_.assign(logonly_replica_attr_array))) {
    LOG_WARN("fail to assign logonly replica attr array", KR(ret));
  } else if (OB_FAIL(encryption_logonly_replica_attr_array_.assign(encryption_logonly_replica_attr_array))) {
    LOG_WARN("fail to assign encryption logonly replica attr array", KR(ret));
  }
  return ret;
}

int ObReplicaAttrSet::set_readonly_replica_attr_array(
    const common::ObIArray<share::ReplicaAttr> &readonly_replica_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(readonly_replica_attr_array_.assign(readonly_replica_attr_array))) {
    LOG_WARN("fail to assign full replica attr array", K(ret));
  }
  return ret;
}

bool ObReplicaAttrSet::has_paxos_replica() const
{
  bool bool_ret = false;
  if (full_replica_attr_array_.count() > 0) {
    bool_ret = true;
  } else if (logonly_replica_attr_array_.count() > 0) {
    bool_ret = true;
  } else if (encryption_logonly_replica_attr_array_.count() > 0) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObReplicaAttrSet::is_specific_readonly_replica() const
{
  bool bool_ret = false;
  if (readonly_replica_attr_array_.count() > 0
      && ObLocalityDistribution::ALL_SERVER_CNT != readonly_replica_attr_array_.at(0).num_) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObReplicaAttrSet::is_allserver_readonly_replica() const
{
  bool bool_ret = false;
  if (readonly_replica_attr_array_.count() > 0
      && ObLocalityDistribution::ALL_SERVER_CNT == readonly_replica_attr_array_.at(0).num_) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObReplicaAttrSet::is_specific_replica_attr() const
{
  bool bool_ret = false;
  for (int64_t i = 0; !bool_ret && i < full_replica_attr_array_.count(); i++) {
    const ReplicaAttr &replica_attr = full_replica_attr_array_.at(i);
    bool_ret = 100 != replica_attr.memstore_percent_;
  }
  for (int64_t i = 0; !bool_ret && i < logonly_replica_attr_array_.count(); i++) {
    const ReplicaAttr &replica_attr = logonly_replica_attr_array_.at(i);
    bool_ret = 100 != replica_attr.memstore_percent_;
  }
  for (int64_t i = 0; !bool_ret && i < readonly_replica_attr_array_.count(); i++) {
    const ReplicaAttr &replica_attr = readonly_replica_attr_array_.at(i);
    bool_ret = 100 != replica_attr.memstore_percent_;
  }
  for (int64_t i = 0; !bool_ret && i < encryption_logonly_replica_attr_array_.count(); i++) {
    const ReplicaAttr &replica_attr = encryption_logonly_replica_attr_array_.at(i);
    bool_ret = 100 != replica_attr.memstore_percent_;
  }
  return bool_ret;
}

int ObReplicaAttrSet::add_full_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    int64_t index = 0;
    for (/* nop */; index < full_replica_attr_array_.count(); ++index) {
      ReplicaAttr &this_replica_attr = full_replica_attr_array_.at(index);
      if (this_replica_attr.memstore_percent_ == replica_attr.memstore_percent_) {
        break; // find
      }
    }
    if (index >= full_replica_attr_array_.count()) {
      if (OB_FAIL(full_replica_attr_array_.push_back(ReplicaAttr(0, replica_attr.memstore_percent_)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (full_replica_attr_array_.count() <= index) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index unexpected", K(ret), K(index),
               "full_replica_attr_array", full_replica_attr_array_.count());
    } else {
      full_replica_attr_array_.at(index).num_ += replica_attr.num_;
    }
  }
  return ret;
}

int ObReplicaAttrSet::add_logonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    if (logonly_replica_attr_array_.count() <= 0) {
      if (OB_FAIL(logonly_replica_attr_array_.push_back(ReplicaAttr(0, replica_attr.memstore_percent_)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (logonly_replica_attr_array_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index unexpected", K(ret),
               "logonly_replica_attr_array_count", logonly_replica_attr_array_.count());
    } else {
      logonly_replica_attr_array_.at(0).num_ += replica_attr.num_;
    }
  }
  return ret;
}

int ObReplicaAttrSet::add_readonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    int64_t index = 0;
    for (/* nop */; index < readonly_replica_attr_array_.count(); ++index) {
      ReplicaAttr &this_replica_attr = readonly_replica_attr_array_.at(index);
      if (this_replica_attr.memstore_percent_ == replica_attr.memstore_percent_) {
        break; // find
      }
    }
    if (index >= readonly_replica_attr_array_.count()) {
      if (OB_FAIL(readonly_replica_attr_array_.push_back(ReplicaAttr(0, replica_attr.memstore_percent_)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (readonly_replica_attr_array_.count() <= index) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index unexpected", K(ret), K(index),
               "readonly_replica_attr_array", readonly_replica_attr_array_.count());
    } else if (ObLocalityDistribution::ALL_SERVER_CNT == readonly_replica_attr_array_.at(index).num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot add readonly replica num on all server readonly", K(ret));
    } else {
      readonly_replica_attr_array_.at(index).num_ += replica_attr.num_;
    }
  }
  return ret;
}

int ObReplicaAttrSet::add_encryption_logonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    if (encryption_logonly_replica_attr_array_.count() <= 0) {
      if (OB_FAIL(encryption_logonly_replica_attr_array_.push_back(
              ReplicaAttr(0, replica_attr.memstore_percent_)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (encryption_logonly_replica_attr_array_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index unexpected", K(ret),
               "encryption_logonly_replica_attr_array_count",
               encryption_logonly_replica_attr_array_.count());
    } else {
      encryption_logonly_replica_attr_array_.at(0).num_ += replica_attr.num_;
    }
  }
  return ret;
}

int ObReplicaAttrSet::sub_full_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    int64_t index = 0;
    for (/* nop */; index < full_replica_attr_array_.count(); ++index) {
      ReplicaAttr &this_replica_attr = full_replica_attr_array_.at(index);
      if (this_replica_attr.memstore_percent_ == replica_attr.memstore_percent_) {
        break; // find
      }
    }
    if (index >= full_replica_attr_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this replica memstore percent not exist", K(ret), K(replica_attr));
    } else if (full_replica_attr_array_.at(index).num_ < replica_attr.num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("full replica num not enough", K(ret),
               "replica_num_in_array", full_replica_attr_array_.at(index).num_, K(replica_attr));
    } else {
      full_replica_attr_array_.at(index).num_ -= replica_attr.num_;
      if (full_replica_attr_array_.at(index).num_ <= 0) {
        if (OB_FAIL(full_replica_attr_array_.remove(index))) {
          LOG_WARN("fail to remove", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReplicaAttrSet::sub_logonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    if (logonly_replica_attr_array_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("logonly replica attr array empty", K(ret));
    } else if (logonly_replica_attr_array_.at(0).num_ < replica_attr.num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("logonly replica num not enough", K(ret),
               "logonly_replica_num_in_array", logonly_replica_attr_array_.at(0).num_,
               K(replica_attr));
    } else {
      logonly_replica_attr_array_.at(0).num_ -= replica_attr.num_;
      if (logonly_replica_attr_array_.at(0).num_ <= 0) {
        if (OB_FAIL(logonly_replica_attr_array_.remove(0))) {
          LOG_WARN("fail to remove", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReplicaAttrSet::sub_readonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    int64_t index = 0;
    for (/* nop */; index < readonly_replica_attr_array_.count(); ++index) {
      ReplicaAttr &this_replica_attr = readonly_replica_attr_array_.at(index);
      if (this_replica_attr.memstore_percent_ == replica_attr.memstore_percent_) {
        break; // find
      }
    }
    if (index >= readonly_replica_attr_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this replica memstore percent not exist", K(ret), K(replica_attr));
    } else if (readonly_replica_attr_array_.at(index).num_ < replica_attr.num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("full replica num not enough", K(ret),
               "replica_num_in_array", readonly_replica_attr_array_.at(index).num_, K(replica_attr));
    } else if (ObLocalityDistribution::ALL_SERVER_CNT == readonly_replica_attr_array_.at(index).num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot subtract all server readonly replica attr", K(ret));
    } else {
      readonly_replica_attr_array_.at(index).num_ -= replica_attr.num_;
      if (readonly_replica_attr_array_.at(index).num_ <= 0) {
        if (OB_FAIL(readonly_replica_attr_array_.remove(index))) {
          LOG_WARN("fail to remove", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReplicaAttrSet::sub_encryption_logonly_replica_num(const ReplicaAttr &replica_attr)
{
  int ret = OB_SUCCESS;
  if (replica_attr.num_ > 0) {
    if (encryption_logonly_replica_attr_array_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encryption logonly replica attr array empty", K(ret));
    } else if (encryption_logonly_replica_attr_array_.at(0).num_ < replica_attr.num_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encryption logonly replica num not enough", K(ret),
               "encryption_logonly_replica_num_in_array",
               encryption_logonly_replica_attr_array_.at(0).num_,
               K(replica_attr));
    } else {
      encryption_logonly_replica_attr_array_.at(0).num_ -= replica_attr.num_;
      if (encryption_logonly_replica_attr_array_.at(0).num_ <= 0) {
        if (OB_FAIL(encryption_logonly_replica_attr_array_.remove(0))) {
          LOG_WARN("fail to remove", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObReplicaAttrSet::has_this_replica(
     const common::ObReplicaType replica_type,
     const int64_t memstore_percent)
{
  bool found = false;
  if (common::REPLICA_TYPE_FULL == replica_type) {
    for (int64_t i = 0; !found && i < full_replica_attr_array_.count(); ++i) {
      ReplicaAttr &this_replica_attr = full_replica_attr_array_.at(i);
      if (this_replica_attr.num_ > 0 && memstore_percent == this_replica_attr.memstore_percent_) {
        found = true;
      }
    }
  } else if (common::REPLICA_TYPE_LOGONLY == replica_type) {
    for (int64_t i = 0; !found && i < logonly_replica_attr_array_.count(); ++i) {
      ReplicaAttr &this_replica_attr = logonly_replica_attr_array_.at(i);
      if (this_replica_attr.num_ > 0 && memstore_percent == this_replica_attr.memstore_percent_) {
        found = true;
      }
    }
  } else if (common::REPLICA_TYPE_READONLY == replica_type) {
    for (int64_t i = 0; !found && i < readonly_replica_attr_array_.count(); ++i) {
      ReplicaAttr &this_replica_attr = readonly_replica_attr_array_.at(i);
      if (this_replica_attr.num_ > 0 && memstore_percent == this_replica_attr.memstore_percent_) {
        found = true;
      }
    }
  } else if (common::REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type) {
    for (int64_t i = 0; !found && i < encryption_logonly_replica_attr_array_.count(); ++i) {
      ReplicaAttr &this_replica_attr = encryption_logonly_replica_attr_array_.at(i);
      if (this_replica_attr.num_ > 0 && memstore_percent == this_replica_attr.memstore_percent_) {
        found = true;
      }
    }
  } else {
    found = false;
  }
  return found;
}


int ObReplicaAttrSet::get_readonly_memstore_percent(int64_t &memstore_percent) const
{
  int ret = OB_SUCCESS;
  if (readonly_replica_attr_array_.count() <= 0 || readonly_replica_attr_array_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("readonly replica attr array count unexpected", K(ret),
             "array_count", readonly_replica_attr_array_.count());
  } else {
    const ReplicaAttr &replica_attr = readonly_replica_attr_array_.at(0);
    memstore_percent = replica_attr.memstore_percent_;
  }
  return ret;
}

int ObZoneReplicaAttrSet::append(const ObZoneReplicaAttrSet &that)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < that.zone_set_.count(); ++i) {
    if (OB_FAIL(zone_set_.push_back(that.zone_set_.at(i)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  lib::ob_sort(zone_set_.begin(), zone_set_.end());
  for (int64_t i = 0;
       OB_SUCC(ret) && i < that.replica_attr_set_.get_full_replica_attr_array().count();
       ++i) {
    const ReplicaAttr &this_replica_attr = that.replica_attr_set_.get_full_replica_attr_array().at(i);
    if (OB_FAIL(add_full_replica_num(this_replica_attr))) {
      LOG_WARN("fail to add full replica num", K(ret));
    }
  }
  lib::ob_sort(replica_attr_set_.get_full_replica_attr_array_for_sort().begin(),
            replica_attr_set_.get_full_replica_attr_array_for_sort().end());
  for (int64_t i = 0;
       OB_SUCC(ret) && i < that.replica_attr_set_.get_logonly_replica_attr_array().count();
       ++i) {
    const ReplicaAttr &this_replica_attr = that.replica_attr_set_.get_logonly_replica_attr_array().at(i);
    if (OB_FAIL(add_logonly_replica_num(this_replica_attr))) {
      LOG_WARN("fail to add logonly replica num", K(ret));
    }
  }
  lib::ob_sort(replica_attr_set_.get_logonly_replica_attr_array_for_sort().begin(),
            replica_attr_set_.get_logonly_replica_attr_array_for_sort().end());
  for (int64_t i = 0;
       OB_SUCC(ret) && i < that.replica_attr_set_.get_readonly_replica_attr_array().count();
       ++i) {
    const ReplicaAttr &this_replica_attr = that.replica_attr_set_.get_readonly_replica_attr_array().at(i);
    if (OB_FAIL(add_readonly_replica_num(this_replica_attr))) {
      LOG_WARN("fail to add readonly replica num", K(ret));
    }
  }
  lib::ob_sort(replica_attr_set_.get_readonly_replica_attr_array_for_sort().begin(),
            replica_attr_set_.get_readonly_replica_attr_array_for_sort().end());
  for (int64_t i = 0;
       OB_SUCC(ret) && i < that.replica_attr_set_.get_encryption_logonly_replica_attr_array().count();
       ++i) {
    const ReplicaAttr &this_replica_attr
      = that.replica_attr_set_.get_encryption_logonly_replica_attr_array().at(i);
    if (OB_FAIL(add_encryption_logonly_replica_num(this_replica_attr))) {
      LOG_WARN("fail to add logonly replica num", K(ret));
    }
  }
  lib::ob_sort(replica_attr_set_.get_encryption_logonly_replica_attr_array_for_sort().begin(),
            replica_attr_set_.get_encryption_logonly_replica_attr_array_for_sort().end());
  return ret;
}

bool ObZoneReplicaAttrSet::sort_compare_less_than(const ObZoneReplicaAttrSet *lset, const ObZoneReplicaAttrSet *rset)
{
  bool bret = false;
  if (OB_ISNULL(lset) || OB_ISNULL(rset)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "left or right is null", KP(lset), KP(rset));
  } else {
    if (lset->zone_set_.count() < rset->zone_set_.count()) {
      bret = true;
    } else if (lset->zone_set_.count() > rset->zone_set_.count()) {
      bret = false;
    } else {
      for (int64_t i = 0; i < lset->zone_set_.count(); ++i) {
        const common::ObZone &this_zone = lset->zone_set_.at(i);
        const common::ObZone &that_zone = rset->zone_set_.at(i);
        if (this_zone == that_zone) {
          // go on next
        } else if (this_zone < that_zone) {
          bret = true;
          break;
        } else {
          bret = false;
          break;
        }
      }
    }
  }
  return bret;
}

bool ObZoneReplicaAttrSet::operator<(
     const ObZoneReplicaAttrSet &that)
{
  return sort_compare_less_than(this, &that);
}

int ObZoneReplicaAttrSet::assign(const ObZoneReplicaAttrSet &that)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(zone_set_.assign(that.zone_set_))) {
    LOG_WARN("fail to assign zone set", K(ret));
  } else if (OB_FAIL(replica_attr_set_.assign(that.replica_attr_set_))) {
    LOG_WARN("fail to assign replica attr set", K(ret));
  } else {
    zone_ = that.zone_;
  }
  return ret;
}

bool ObZoneReplicaAttrSet::operator==(const ObZoneReplicaAttrSet &that) const
{
  bool bool_ret = true;
  if (replica_attr_set_ != that.replica_attr_set_) {
    bool_ret = false;
  } else {
    if (zone_set_.count() != that.zone_set_.count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < zone_set_.count(); ++i) {
        if (zone_set_.at(i) != that.zone_set_.at(i)) {
          bool_ret = false;
        }
      }
    }
  }
  return bool_ret;
}

bool ObZoneReplicaAttrSet::is_zone_set_match(const ObZoneReplicaAttrSet &that) const
{
  bool bool_ret = true;
  if (zone_set_.count() != that.zone_set_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < zone_set_.count(); ++i) {
      if (zone_set_.at(i) != that.zone_set_.at(i)) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

bool ObZoneReplicaAttrSet::is_paxos_locality_match(const ObZoneReplicaAttrSet &that) const
{
  bool equal = true;
  if (replica_attr_set_.get_full_replica_attr_array().count() != that.replica_attr_set_.get_full_replica_attr_array().count()
      || replica_attr_set_.get_logonly_replica_attr_array().count() != that.replica_attr_set_.get_logonly_replica_attr_array().count()
      || replica_attr_set_.get_encryption_logonly_replica_attr_array().count() != that.replica_attr_set_.get_encryption_logonly_replica_attr_array().count()) {
    equal = false;
  } else {
    for (int64_t i = 0; equal && i < replica_attr_set_.get_full_replica_attr_array().count(); ++i) {
      if (replica_attr_set_.get_full_replica_attr_array().at(i) != that.replica_attr_set_.get_full_replica_attr_array().at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < replica_attr_set_.get_logonly_replica_attr_array().count(); ++i) {
      if (replica_attr_set_.get_logonly_replica_attr_array().at(i) != that.replica_attr_set_.get_logonly_replica_attr_array().at(i)) {
        equal = false;
      }
    }
    for (int64_t i = 0; equal && i < replica_attr_set_.get_encryption_logonly_replica_attr_array().count(); ++i) {
      if (replica_attr_set_.get_encryption_logonly_replica_attr_array().at(i) != that.replica_attr_set_.get_encryption_logonly_replica_attr_array().at(i)) {
        equal = false;
      }
    }
  }
  return equal;
}

bool ObZoneReplicaAttrSet::is_alter_locality_match(
     const ObZoneReplicaAttrSet &that) const
{
  int64_t max_alter_num = 0;
  bool match = true;
  for (int64_t i = 0; i < replica_attr_set_.get_full_replica_attr_array().count(); ++i) {
    int64_t index_num = 0;
    int64_t tmp_pre_num = replica_attr_set_.get_full_replica_attr_array().at(i).num_;
    int64_t tmp_pre_memstore_percent = replica_attr_set_.get_full_replica_attr_array().at(i).memstore_percent_;
    for (int64_t j = 0; j < that.replica_attr_set_.get_full_replica_attr_array().count(); ++j) {
      int64_t tmp_cur_num = that.replica_attr_set_.get_full_replica_attr_array().at(j).num_;
      int64_t tmp_cur_memstore_percent = that.replica_attr_set_.get_full_replica_attr_array().at(j).memstore_percent_;
      if (tmp_pre_memstore_percent == tmp_cur_memstore_percent) {
        if (tmp_pre_num > tmp_cur_num) {
          max_alter_num += (tmp_pre_num - tmp_cur_num);//Only record positive changes
        }
        break;
      }
      index_num++;
    }
    if (index_num == that.replica_attr_set_.get_full_replica_attr_array().count()) {//There is no corresponding m_p in that, add the corresponding num of m_p to the number of changes
      max_alter_num += tmp_pre_num;
    }
  }
  if (replica_attr_set_.get_logonly_replica_num() > 
      that.get_logonly_replica_num()) {//Also only record positive values
    max_alter_num += (replica_attr_set_.get_logonly_replica_num() - 
                      that.get_logonly_replica_num());
  }
  if (replica_attr_set_.get_encryption_logonly_replica_num() > 
      that.get_encryption_logonly_replica_num()) {//Also only record positive values
    max_alter_num += (replica_attr_set_.get_encryption_logonly_replica_num() - 
                      that.get_encryption_logonly_replica_num());
  }
  if (max_alter_num > 1) { //Determine whether the value is greater than 1, and no more than 1 change is allowed
    match = false;
  }
  return match;
}

bool ObZoneReplicaAttrSet::check_paxos_num_valid() const
{
  return (0 <= get_full_replica_num()
         && 1 >= get_full_replica_num()
         && 0 <= get_logonly_replica_num()
         && 1 >= get_logonly_replica_num()
         && 0 <= get_encryption_logonly_replica_num()
         && 1 >= get_encryption_logonly_replica_num()
         && 0 <= get_paxos_replica_num()
         && 2 >= get_paxos_replica_num());
}


void ObReplicaNumSet::set_replica_num(
    int64_t full_replica_num,
    int64_t logonly_replica_num,
    int64_t readonly_replica_num,
    int64_t encryption_logonly_replica_num)
{
  full_replica_num_ = full_replica_num;
  logonly_replica_num_ = logonly_replica_num;
  readonly_replica_num_ = readonly_replica_num;
  encryption_logonly_replica_num_ = encryption_logonly_replica_num;
}

int64_t ObReplicaNumSet::get_specific_replica_num() const
{
  int64_t specific_replica_num = 0;
  if (ObLocalityDistribution::ALL_SERVER_CNT != full_replica_num_) {
    specific_replica_num += full_replica_num_;
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != logonly_replica_num_) {
    specific_replica_num += logonly_replica_num_;
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != readonly_replica_num_) {
    specific_replica_num += readonly_replica_num_;
  }
  if (ObLocalityDistribution::ALL_SERVER_CNT != encryption_logonly_replica_num_) {
    specific_replica_num += encryption_logonly_replica_num_;
  }
  return specific_replica_num;
}

int64_t SchemaReplicaAttrSet::get_convert_size() const
{
  int64_t convert_size = sizeof(SchemaReplicaAttrSet);
  const ObIArray<ReplicaAttr> &full_set = get_full_replica_attr_array();
  convert_size += full_set.count() * static_cast<int64_t>(sizeof(share::ReplicaAttr));
  const ObIArray<ReplicaAttr> &logonly_set = get_logonly_replica_attr_array();
  convert_size += logonly_set.count() * static_cast<int64_t>(sizeof(share::ReplicaAttr));
  const ObIArray<ReplicaAttr> &readonly_set = get_readonly_replica_attr_array();
  convert_size += readonly_set.count() * static_cast<int64_t>(sizeof(share::ReplicaAttr));
  const ObIArray<ReplicaAttr> &encryption_logonly_set = get_encryption_logonly_replica_attr_array();
  convert_size += encryption_logonly_set.count() * static_cast<int64_t>(sizeof(share::ReplicaAttr));
  return convert_size;
}

int64_t SchemaZoneReplicaAttrSet::get_convert_size() const
{
  int64_t convert_size = sizeof(SchemaZoneReplicaAttrSet);
  convert_size += (replica_attr_set_.get_convert_size() - sizeof(SchemaReplicaAttrSet));
  convert_size += zone_set_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t j = 0; j < zone_set_.count(); ++j) {
    convert_size += zone_set_.at(j).length() + 1;
  }
  return convert_size;
}

} // end namespace share
} // end namespace oceanbase
