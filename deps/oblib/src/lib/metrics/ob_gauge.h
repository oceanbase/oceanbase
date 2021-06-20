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

#ifndef _OB_GAUGE_H
#define _OB_GAUGE_H 1
#include <stdint.h>
namespace oceanbase {
namespace common {
// A gauge just returns a value.
class ObGauge {
public:
  virtual ~ObGauge(){};
  virtual int64_t get_value() const = 0;
};

}  // end namespace common
}  // end namespace oceanbase

#endif /* _OB_GAUGE_H */
