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

#ifndef OCEANBASE_SHARE_OB_COMMON_ID_H_
#define OCEANBASE_SHARE_OB_COMMON_ID_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
#include "share/ob_display_list.h"          // ObDisplayType

namespace oceanbase
{
namespace share
{

// Define a common ID type for all ID requirement.
class ObCommonID
{
public:
  static const int64_t INVALID_ID = -1;

public:
  ObCommonID() : id_(INVALID_ID) {}
  ObCommonID(const ObCommonID &other) : id_(other.id_) {}
  explicit ObCommonID(const int64_t id) : id_(id) {}
  ~ObCommonID() { reset(); }

public:
  int64_t id() const { return id_; }
  void reset() { id_ = INVALID_ID; }

  // assignment
  ObCommonID &operator=(const int64_t id) { id_ = id; return *this; }
  ObCommonID &operator=(const ObCommonID &other) { id_ = other.id_; return *this; }

  bool is_valid() const { return INVALID_ID != id_; }

  // compare operator
  bool operator == (const ObCommonID &other) const { return id_ == other.id_; }
  bool operator >  (const ObCommonID &other) const { return id_ > other.id_; }
  bool operator != (const ObCommonID &other) const { return id_ != other.id_; }
  bool operator <  (const ObCommonID &other) const { return id_ < other.id_; }
  bool operator <= (const ObCommonID &other) const { return id_ <= other.id_; }
  bool operator >= (const ObCommonID &other) const { return id_ >= other.id_; }
  int compare(const ObCommonID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }

  /////////////////////// for ObDisplayType ///////////////////////
  //NOTE: to use ObDisplayList, we should implement all interfaces of ObDisplayType
  //
  // max length of "id": 20 + '\0'
  int64_t max_display_str_len() const { return 21; }
  // convert to "id"
  int to_display_str(char *buf, const int64_t len, int64_t &pos) const;
  // parse from "id"
  int parse_from_display_str(const common::ObString &str);

  uint64_t hash() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));
private:
  int64_t id_;
};

}
}

#endif /* OCEANBASE_SHARE_OB_COMMON_ID_H_ */
