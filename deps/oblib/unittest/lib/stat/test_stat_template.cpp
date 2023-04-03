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

#include <gtest/gtest.h>
#include "lib/stat/ob_stat_template.h"
#include "lib/stat/ob_session_stat.h"

namespace oceanbase
{
namespace common
{
TEST(ObStatArray, normal)
{
  int ret = OB_SUCCESS;
  ObWaitEventStatArray stats1;
  ObWaitEventStatArray stats2;
  ObWaitEventStatArray::Iterator iter;
  const ObWaitEventStat *item = NULL;

  stats1.get(0)->total_waits_ = 1;
  stats1.get(3)->total_waits_ = 2;
  stats2.get(4)->total_waits_ = 3;
  stats1.add(stats2);

  stats1.get_iter(iter);
  ret = iter.get_next(item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, item->total_waits_);
  ret = iter.get_next(item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, item->total_waits_);
  ret = iter.get_next(item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, item->total_waits_);
  ret = iter.get_next(item);
  ASSERT_EQ(OB_ITER_END, ret);


}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}





