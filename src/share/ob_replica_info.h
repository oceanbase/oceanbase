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

#ifndef OCEANBASE_SHARE_OB_REPLICA_INFO_H_
#define OCEANBASE_SHARE_OB_REPLICA_INFO_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_helper.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"

namespace oceanbase
{
namespace share
{
struct ReplicaAttr
{
  ReplicaAttr() : num_(0), memstore_percent_(100) {}
  ReplicaAttr(const int64_t num, const int64_t memstore_percent)
    : num_(num),
      memstore_percent_(memstore_percent) {}
  TO_STRING_KV(K_(num), K_(memstore_percent));
  bool operator<(const ReplicaAttr &that) {
    bool bool_ret = true;
    if (memstore_percent_ > that.memstore_percent_) {
      bool_ret = true;
    } else if (memstore_percent_ < that.memstore_percent_) {
      bool_ret = false;
    } else {
      if (num_ > that.num_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
    }
    return bool_ret;
  }
  bool is_valid() const { return num_ >= 0 && memstore_percent_ >= 0 && memstore_percent_ <= 100; }
  void reset() { num_ = 0, memstore_percent_ = 100; }
  bool operator==(const ReplicaAttr &that) const {
    return num_ == that.num_
           && memstore_percent_ == that.memstore_percent_;
  }
  bool operator!=(const ReplicaAttr &that) const {
    return !(*this == that);
  }
  int64_t num_;
  int64_t memstore_percent_;
};

class BaseReplicaAttrSet
{
public:
  BaseReplicaAttrSet() {}
  virtual ~BaseReplicaAttrSet() {}

public:
  virtual const common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() const = 0;
  virtual const common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() const = 0;
  virtual const common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() const = 0;
  virtual const common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() const = 0;
  virtual common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() = 0;
  virtual common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() = 0;
  virtual common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() = 0;
  virtual common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() = 0;

  int64_t get_full_replica_num() const;
  int64_t get_logonly_replica_num() const;
  int64_t get_readonly_replica_num() const;
  int64_t get_encryption_logonly_replica_num() const;
  int64_t get_paxos_replica_num() const;
  int64_t get_specific_replica_num() const;

  TO_STRING_KV("full_replica_attr_array", get_full_replica_attr_array(),
               "logonly_replica_attr_array", get_logonly_replica_attr_array(),
               "readonly_replica_attr_array", get_readonly_replica_attr_array(),
               "encryption_logonly_replica_attr_array", get_encryption_logonly_replica_attr_array());
};

typedef common::ObArrayHelper<share::ReplicaAttr> SchemaReplicaAttrArray;
typedef common::ObArray<share::ReplicaAttr> ReplicaAttrArray;

class SchemaReplicaAttrSet : public BaseReplicaAttrSet
{
public:
  SchemaReplicaAttrSet() : BaseReplicaAttrSet(),
                           full_replica_attr_array_(),
                           logonly_replica_attr_array_(),
                           readonly_replica_attr_array_(),
                           encryption_logonly_replica_attr_array_() {}
  virtual ~SchemaReplicaAttrSet() {}
  int64_t get_convert_size() const;
public:
  virtual const common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() const override {
    return full_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() const override {
    return logonly_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() const override {
    return readonly_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() const override {
    return encryption_logonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() override {
    return full_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() override {
    return logonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() override {
    return readonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() override {
    return encryption_logonly_replica_attr_array_;
  }
public:
  void reset() {
    full_replica_attr_array_.reset();
    logonly_replica_attr_array_.reset();
    readonly_replica_attr_array_.reset();
    encryption_logonly_replica_attr_array_.reset();
  }
private:
  SchemaReplicaAttrArray full_replica_attr_array_;
  SchemaReplicaAttrArray logonly_replica_attr_array_;
  SchemaReplicaAttrArray readonly_replica_attr_array_;
  SchemaReplicaAttrArray encryption_logonly_replica_attr_array_;
};

class ObReplicaAttrSet : public BaseReplicaAttrSet
{
public:
  ObReplicaAttrSet() : BaseReplicaAttrSet(),
                       full_replica_attr_array_(),
                       logonly_replica_attr_array_(),
                       readonly_replica_attr_array_(),
                       encryption_logonly_replica_attr_array_() {}
  virtual ~ObReplicaAttrSet() {}
  bool operator==(const ObReplicaAttrSet &that) const;
  bool is_paxos_replica_match(const ObReplicaAttrSet &that) const;
  bool operator!=(const ObReplicaAttrSet &that) const;
  int assign(const BaseReplicaAttrSet &that);
  void reset() {
    full_replica_attr_array_.reset();
    logonly_replica_attr_array_.reset();
    readonly_replica_attr_array_.reset();
    encryption_logonly_replica_attr_array_.reset();
  }

  int add_full_replica_num(const ReplicaAttr &replica_attr);
  int add_logonly_replica_num(const ReplicaAttr &replica_attr);
  int add_readonly_replica_num(const ReplicaAttr &replica_attr);
  int add_encryption_logonly_replica_num(const ReplicaAttr &replica_attr);
  
  int sub_full_replica_num(const ReplicaAttr &replica_attr);
  int sub_logonly_replica_num(const ReplicaAttr &replica_attr);
  int sub_readonly_replica_num(const ReplicaAttr &replica_attr);
  int sub_encryption_logonly_replica_num(const ReplicaAttr &replica_attr);
  
  bool has_this_replica(
       const common::ObReplicaType replica_type,
       const int64_t memstore_percent);
  
  virtual const common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() const override {
    return full_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() const override {
    return logonly_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() const override {
    return readonly_replica_attr_array_;
  }
  virtual const common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() const override {
    return encryption_logonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_full_replica_attr_array() override {
    return full_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_logonly_replica_attr_array() override {
    return logonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_readonly_replica_attr_array() override {
    return readonly_replica_attr_array_;
  }
  virtual common::ObIArray<ReplicaAttr> &get_encryption_logonly_replica_attr_array() override {
    return encryption_logonly_replica_attr_array_;
  }

  ReplicaAttrArray &get_full_replica_attr_array_for_sort() {
    return full_replica_attr_array_;
  }
  ReplicaAttrArray &get_logonly_replica_attr_array_for_sort() {
    return logonly_replica_attr_array_;
  }
  ReplicaAttrArray &get_readonly_replica_attr_array_for_sort() {
    return readonly_replica_attr_array_;
  }
  ReplicaAttrArray &get_encryption_logonly_replica_attr_array_for_sort() {
    return encryption_logonly_replica_attr_array_;
  }

  int set_replica_attr_array(
      const common::ObIArray<share::ReplicaAttr> &full_replica_attr_array,
      const common::ObIArray<share::ReplicaAttr> &logonly_replica_attr_array,
      const common::ObIArray<share::ReplicaAttr> &readonly_replica_attr_array,
      const common::ObIArray<share::ReplicaAttr> &encryption_logonly_replica_attr_array);

  int set_paxos_replica_attr_array(
      const common::ObIArray<share::ReplicaAttr> &full_replica_attr_array,
      const common::ObIArray<share::ReplicaAttr> &logonly_replica_attr_array,
      const common::ObIArray<share::ReplicaAttr> &encryption_logonly_replica_attr_array);

  int set_readonly_replica_attr_array(
      const common::ObIArray<share::ReplicaAttr> &readonly_replica_attr_array);

  int get_readonly_memstore_percent(int64_t &memstore_percent) const;

  bool has_paxos_replica() const;
  bool is_specific_readonly_replica() const;
  bool is_allserver_readonly_replica() const;
  bool is_specific_replica_attr() const;

private:
  ReplicaAttrArray full_replica_attr_array_;
  ReplicaAttrArray logonly_replica_attr_array_;
  ReplicaAttrArray readonly_replica_attr_array_;
  ReplicaAttrArray encryption_logonly_replica_attr_array_;
};

struct SchemaZoneReplicaAttrSet
{
  SchemaZoneReplicaAttrSet() : replica_attr_set_(),
                               zone_set_(),
                               zone_() {}
  virtual ~SchemaZoneReplicaAttrSet() {}
  int64_t get_convert_size() const;
  int64_t get_full_replica_num() const {return replica_attr_set_.get_full_replica_num();}
  int64_t get_logonly_replica_num() const {return replica_attr_set_.get_logonly_replica_num();}
  int64_t get_readonly_replica_num() const {return replica_attr_set_.get_readonly_replica_num();}
  int64_t get_encryption_logonly_replica_num() const {return replica_attr_set_.get_encryption_logonly_replica_num();}
  int64_t get_paxos_replica_num() const {
    return get_full_replica_num() + get_logonly_replica_num() + get_encryption_logonly_replica_num();
  }
  int64_t get_non_readonly_replica_num() const {
    return get_paxos_replica_num();
  }
  int64_t get_specific_replica_num() const {
    return replica_attr_set_.get_specific_replica_num();
  }
  TO_STRING_KV(K_(replica_attr_set), K_(zone_set), K_(zone));

  SchemaReplicaAttrSet replica_attr_set_;
  common::ObArrayHelper<common::ObString> zone_set_;
  common::ObZone zone_;
};

struct ObZoneReplicaAttrSet
{
  ObZoneReplicaAttrSet() : zone_(), replica_attr_set_(), zone_set_() {}
  virtual ~ObZoneReplicaAttrSet() {}
  void reset() { zone_.reset(); replica_attr_set_.reset(); zone_set_.reset(); }
  int assign(const ObZoneReplicaAttrSet &that);
  bool operator==(const ObZoneReplicaAttrSet &that) const;
  bool is_zone_set_match(const ObZoneReplicaAttrSet &that) const;
  bool is_paxos_locality_match(const ObZoneReplicaAttrSet &that) const;
  bool is_alter_locality_match(
       const ObZoneReplicaAttrSet &that) const;
  bool operator!=(const ObZoneReplicaAttrSet &that) const {
    return (!(*this == that));
  }
  bool operator<(const ObZoneReplicaAttrSet &that);
  static bool sort_compare_less_than(const ObZoneReplicaAttrSet *lset, const ObZoneReplicaAttrSet *rset);
  int64_t get_full_replica_num() const {return replica_attr_set_.get_full_replica_num();}
  int64_t get_logonly_replica_num() const {return replica_attr_set_.get_logonly_replica_num();}
  int64_t get_readonly_replica_num() const {return replica_attr_set_.get_readonly_replica_num();}
  int64_t get_encryption_logonly_replica_num() const {
    return replica_attr_set_.get_encryption_logonly_replica_num();
  }
  const common::ObIArray<common::ObZone> &get_zone_set() const { return zone_set_; }

  int add_full_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.add_full_replica_num(replica_attr);
  }
  int add_logonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.add_logonly_replica_num(replica_attr);
  }
  int add_readonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.add_readonly_replica_num(replica_attr);
  }
  int add_encryption_logonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.add_encryption_logonly_replica_num(replica_attr);
  }

  int sub_full_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.sub_full_replica_num(replica_attr);
  }
  int sub_logonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.sub_logonly_replica_num(replica_attr);
  }
  int sub_readonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.sub_readonly_replica_num(replica_attr);
  }
  int sub_encryption_logonly_replica_num(const ReplicaAttr &replica_attr) {
    return replica_attr_set_.sub_encryption_logonly_replica_num(replica_attr);
  }

  int64_t get_paxos_replica_num() const {
    return get_full_replica_num() + get_logonly_replica_num() + get_encryption_logonly_replica_num();
  }
  int64_t get_non_readonly_replica_num() const {
    return get_paxos_replica_num();
  }
  bool has_non_paxos_replica() const {
    return (get_readonly_replica_num() > 0);
  }
  int64_t get_specific_replica_num() const {
    return replica_attr_set_.get_specific_replica_num();
  }

  bool has_paxos_replica() const {
    return replica_attr_set_.has_paxos_replica();
  }
  bool is_specific_readonly_replica() const {
    return replica_attr_set_.is_specific_readonly_replica();
  }
  bool is_allserver_readonly_replica() const {
    return replica_attr_set_.is_allserver_readonly_replica();
  }
  bool is_mixed_locality() const {
    return zone_set_.count() > 1;
  }
  bool is_specific_replica_attr() const {
    return replica_attr_set_.is_specific_replica_attr();
  }

  int append(const share::ObZoneReplicaAttrSet &that);

  bool check_paxos_num_valid() const;
  TO_STRING_KV(K_(zone), K_(replica_attr_set), K_(zone_set));
  common::ObZone zone_; // For the time being, for the interface to be compiled and passed, it will be removed after the schema split is completed.
  ObReplicaAttrSet replica_attr_set_;
  common::ObArray<common::ObZone> zone_set_;
};

typedef ObZoneReplicaAttrSet ObZoneReplicaNumSet;

struct ObReplicaNumSet
{
  ObReplicaNumSet() : full_replica_num_(0),
                      logonly_replica_num_(0),
                      readonly_replica_num_(0),
                      encryption_logonly_replica_num_(0)
  { reset(); }
  virtual ~ObReplicaNumSet() {}
  bool operator==(const ObReplicaNumSet &that) {
    return full_replica_num_ == that.full_replica_num_
           && logonly_replica_num_ == that.logonly_replica_num_
           && readonly_replica_num_ == that.readonly_replica_num_
           && encryption_logonly_replica_num_ == that.encryption_logonly_replica_num_;
  }
  bool operator!=(const ObReplicaNumSet &that) {
    return (!(*this == that));
  }
  void reset() {
    full_replica_num_ = 0;
    logonly_replica_num_ = 0;
    readonly_replica_num_ = 0;
    encryption_logonly_replica_num_ = 0;
  }
  ObReplicaNumSet &operator=(const ObReplicaNumSet &that) {
    if (this != &that) {
      full_replica_num_ = that.full_replica_num_;
      logonly_replica_num_ = that.logonly_replica_num_;
      readonly_replica_num_ = that.readonly_replica_num_;
      encryption_logonly_replica_num_ = that.encryption_logonly_replica_num_;
    }
    return *this;
  }
  void set_replica_num(
      int64_t full_replica_num,
      int64_t logonly_replica_num,
      int64_t readonly_replica_num,
      int64_t encryption_logonly_replica_num);

  int64_t get_specific_replica_num() const;

  TO_STRING_KV(K(full_replica_num_),
               K(logonly_replica_num_),
               K(readonly_replica_num_),
               K(logonly_replica_num_));

  int64_t full_replica_num_;
  int64_t logonly_replica_num_;
  int64_t readonly_replica_num_;
  int64_t encryption_logonly_replica_num_;
};

} // end namespace share
} // end namespace oceanbase
#endif
