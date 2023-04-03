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

#ifndef OB_MICRO_BLOCK_ENCRYPTION_H_
#define OB_MICRO_BLOCK_ENCRYPTION_H_
#include "ob_data_buffer.h"

namespace oceanbase {

namespace blocksstable {

class ObMicroBlockEncryption
{
public:
  ObMicroBlockEncryption() {}
  virtual ~ObMicroBlockEncryption() {}
DISALLOW_COPY_AND_ASSIGN(ObMicroBlockEncryption);
};

}
}
#endif // OB_MICRO_BLOCK_ENCRYPTION_H_
