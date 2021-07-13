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

#ifndef OB_I_WORD_SEGMENT_IMPL_H_
#define OB_I_WORD_SEGMENT_IMPL_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "common/object/ob_object.h"

namespace oceanbase {
namespace common {

class ObIWordSegmentImpl {
public:
  ObIWordSegmentImpl(){};
  virtual ~ObIWordSegmentImpl()
  {}

  virtual int init() = 0;
  virtual int segment(const ObString& string, ObIArray<ObString>& words) = 0;
  virtual int segment(const ObObj& string, ObIArray<ObObj>& words) = 0;
  virtual int reset() = 0;
  virtual int destory() = 0;
};

}  // namespace common
}  // namespace oceanbase

#endif /* OB_I_WORD_SEGMENT_IMPL_H_ */
