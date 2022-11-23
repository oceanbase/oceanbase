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
#define private public
#include "lib/atomic/ob_atomic_reference.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

TEST(ObAtomicReference, normal)
{
  int ret = OB_SUCCESS;
  uint32_t ref_cnt = 0;
  ObAtomicReference atomic_ref;

  //test overflow
  atomic_ref.atomic_num_.ref = UINT32_MAX;
  ret = atomic_ref.inc_ref_cnt();
  ASSERT_NE(OB_SUCCESS, ret);
  ret = atomic_ref.check_seq_num_and_inc_ref_cnt(0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = atomic_ref.check_and_inc_ref_cnt();
  ASSERT_NE(OB_SUCCESS, ret);

  //test 0
  atomic_ref.reset();
  ret = atomic_ref.dec_ref_cnt_and_inc_seq_num(ref_cnt);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = atomic_ref.check_and_inc_ref_cnt();
  ASSERT_NE(OB_SUCCESS, ret);
  ret = atomic_ref.check_seq_num_and_inc_ref_cnt(1);
  ASSERT_NE(OB_SUCCESS, ret);

  //test normal
  ret = atomic_ref.inc_ref_cnt();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = atomic_ref.check_and_inc_ref_cnt();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = atomic_ref.check_seq_num_and_inc_ref_cnt(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = atomic_ref.dec_ref_cnt_and_inc_seq_num(ref_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = atomic_ref.dec_ref_cnt_and_inc_seq_num(ref_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = atomic_ref.dec_ref_cnt_and_inc_seq_num(ref_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1U, atomic_ref.get_seq_num());
}
}
}


int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
