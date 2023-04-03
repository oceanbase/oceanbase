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

#ifndef __OB_SHARE_SEQUENCE_SEQUENCE_OPTION_CHECKER_H__
#define __OB_SHARE_SEQUENCE_SEQUENCE_OPTION_CHECKER_H__

#include "lib/container/ob_bit_set.h"

namespace oceanbase
{

namespace share
{
class ObSequenceOption;
class ObSequenceOptionBuilder
{
public:
  ObSequenceOptionBuilder() = default;
  ~ObSequenceOptionBuilder() = default;
  static int build_create_sequence_option(const common::ObBitSet<> &opt_bitset,
                                          share::ObSequenceOption &opt_new);
  static int build_alter_sequence_option(const common::ObBitSet<> &opt_bitset,
                                         const share::ObSequenceOption &opt_old,
                                         share::ObSequenceOption &opt_new,
                                         bool can_alter_start_with);
private:
  static int pre_check_sequence_option(const share::ObSequenceOption &opt);
  static int check_sequence_option(const common::ObBitSet<> &opt_bitset,
                                   const share::ObSequenceOption &opt);
  static int check_sequence_option_integer(const common::ObBitSet<> &opt_bitset,
                                           const share::ObSequenceOption &option);
  DISALLOW_COPY_AND_ASSIGN(ObSequenceOptionBuilder);
};

}
}
#endif /* __OB_SHARE_SEQUENCE_SEQUENCE_OPTION_CHECKER_H__ */
//// end of header file

