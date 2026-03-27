/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
