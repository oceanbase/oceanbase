/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_LIB_REPLICA_DEFINE_H_
#define OB_LIB_REPLICA_DEFINE_H_

#include <stdint.h>
#include <stdlib.h>

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

class ObReplicaProperty
{
  OB_UNIS_VERSION(1);
public:
  ObReplicaProperty() : memstore_percent_(100), reserved_(0) {}

  static ObReplicaProperty create_property(int64_t memstore_percent)
  {
    ObReplicaProperty tmp;
    tmp.memstore_percent_ = memstore_percent & 0x7f;

    return tmp;
  }

  int set_memstore_percent(int64_t memstore_percent);
  int64_t get_memstore_percent() const { return memstore_percent_; }

  bool is_valid() const
  {
    return memstore_percent_ <= 100;
  }

  void reset()
  {
    memstore_percent_ = 100;
  }

  bool operator ==(const ObReplicaProperty &o) const { return property_ == o.property_; }

  TO_STRING_KV(K(memstore_percent_));

private:
  union {
    struct {
      uint64_t memstore_percent_ : 7; // 0-100
      uint64_t reserved_ : 57;
    };
    uint64_t property_;
  };
};

} // common
} // oceanbase

#endif /* OB_LIB_REPLICA_DEFINE_H_ */
