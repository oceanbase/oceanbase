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

#ifndef OB_TABLE_LOAD_INDEX_LONG_WAIT_H_
#define OB_TABLE_LOAD_INDEX_LONG_WAIT_H_

#include "lib/utility/utility.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadIndexLongWait
{
public:
  ObTableLoadIndexLongWait(int64_t wait_us, int64_t max_wait_us)
    : wait_us_(wait_us), max_wait_us_(max_wait_us) {}
  virtual ~ObTableLoadIndexLongWait() {}

  void wait()
  {
    if (wait_us_ < max_wait_us_) {
      ob_usleep(wait_us_);
      wait_us_ = 2 * wait_us_;
    } else {
      ob_usleep(max_wait_us_);
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadIndexLongWait);

private:
  // data members
  int64_t wait_us_;
  int64_t max_wait_us_;
};

}
}

#endif /* OB_TABLE_LOAD_INDEX_LONG_WAIT_H_ */
