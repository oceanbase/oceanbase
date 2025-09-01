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

#ifndef OCEANBASE_SHARE_OB_SEMISTRUCT_PROPERTIES_H_
#define OCEANBASE_SHARE_OB_SEMISTRUCT_PROPERTIES_H_

#include "lib/json_type/ob_json_base.h"

namespace oceanbase
{
namespace share
{

class ObSemistructProperties
{
public:
  enum Mode {
      NONE = 0,
      ENCODING = 1,
      MAX_MODE
  };

  static const uint8_t DEFAULT_SEMISTRUCT_FREQ_THRESHOLD = 100;

public:
  static bool is_mode_valid(int64_t mode) { return mode >= NONE && mode < MAX_MODE; }
  static int merge_new_semistruct_properties(ObIAllocator &allocator,
                                             const common::ObString &alter_properties,
                                             const common::ObString &origin_properties,
                                             common::ObString &new_properties);

  static int check_alter_encoding_type(const common::ObString &origin_properties, bool &contain_encoding_type, uint64_t &encoding_type);

public:
  ObSemistructProperties() :
    mode_(NONE),
    freq_threshold_(DEFAULT_SEMISTRUCT_FREQ_THRESHOLD)
  {}
  void reset() {
    mode_ = NONE;
    freq_threshold_ = DEFAULT_SEMISTRUCT_FREQ_THRESHOLD;
  }

  bool is_enable_semistruct_encoding() const { return mode_ > NONE && mode_ < MAX_MODE; }
  uint8_t get_freq_threshold() const { return freq_threshold_; }
  uint8_t get_mode() const { return mode_; }

  int resolve_semistruct_properties(uint8_t mode, const common::ObString &semistruct_properties);

  TO_STRING_KV(K_(mode), K_(freq_threshold));

private:
  uint8_t mode_;
  uint8_t freq_threshold_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_SEMISTRUCT_PROPERTIES_H_