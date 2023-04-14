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
 *
 * Test ObLogDlist
 * 1. This file is used to test the functionality of (ob_log_dlist.h), path: src/logservice/libobcdc/src/ob_log_dlist
 * 2. The single test of test_ob_log_dlist starts from the following two aspects.
 * (1) basic functional tests
 * (2) Boundary tests
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include <gtest/gtest.h>
#include "share/ob_define.h"
#define private public
#include "logservice/libobcdc/src/ob_log_dlist.h"
#include "ob_log_utils.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{
class DeriveDlistNode;
typedef ObLogDListNode<DeriveDlistNode> TestDlistNode;

class DeriveDlistNode : public TestDlistNode
{
public:
	DeriveDlistNode() : value_(0) {}
	~DeriveDlistNode() {}
public:
  void reset(int64_t value)
	{
	  value_ = value;
	}

private:
	int64_t value_;
};
typedef DeriveDlistNode Type;
// test count
static const int64_t ONE_TEST_COUNT = 1;
static const int64_t MUL_TEST_COUNT = 1000;

class TestObLogDlist: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public:
	// generate data
  void generate_data(const int64_t count, Type *&datas);
	// check data correct
	bool is_dlist_correct(const int64_t start_value, DeriveDlistNode *head);
};

void TestObLogDlist::generate_data(const int64_t count, Type *&datas)
{
	datas = (Type *)ob_malloc(sizeof(Type) * count, ObNewModIds::TEST);
	OB_ASSERT(NULL != datas);
	for (int64_t idx = 0; idx < count; idx++) {
		new (datas + idx) Type();
		datas[idx].reset(idx);
	}
}

bool TestObLogDlist::is_dlist_correct(const int64_t start_value, DeriveDlistNode *head)
{
	bool bool_ret = true;
	int64_t expect_val = start_value;

	if (OB_ISNULL(head) || OB_NOT_NULL(head->get_prev())) {
		LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument");
		bool_ret = false;
	} else if (OB_ISNULL(head->get_next())) { // single node
		if (expect_val != head->value_) {
			bool_ret = false;
		}
		LOG_DEBUG("is_dlist_correct", K(expect_val));
	} else {                                 // multi node
		DeriveDlistNode *current_node = head;
		DeriveDlistNode *next_node = current_node->get_next();
    while ((NULL != current_node)
				    && (NULL != current_node->get_next())) {
			if ((expect_val != current_node->value_)
					|| (expect_val != next_node->get_prev()->value_)) {
				bool_ret = false;
			}
			LOG_DEBUG("is_dlist_correct", K(expect_val));
			current_node = next_node;
			next_node = current_node->get_next();
			expect_val--;
		}
		// last node
		if ((expect_val == current_node->value_)
				 && OB_ISNULL(current_node->get_next())) {
			LOG_DEBUG("is_dlist_correct", K(expect_val));
		} else {
			bool_ret = false;
		}
	}

	return bool_ret;
}

////////////////////// basic functions //////////////////////////////////////////
TEST_F(TestObLogDlist, dlist)
{
	// generate data
	Type *datas = NULL;
	generate_data(MUL_TEST_COUNT, datas);

  // ObLogDList
	ObLogDList<DeriveDlistNode> dlist;
	EXPECT_EQ(0, dlist.count_);
	EXPECT_EQ(NULL, dlist.head_);

	// insert data
	dlist.add_head(datas[0]);
	EXPECT_EQ(ONE_TEST_COUNT, dlist.count_);
	EXPECT_TRUE(is_dlist_correct(ONE_TEST_COUNT - 1, dlist.head()));

	// insert multi data
	for (int64_t idx = 1; idx < MUL_TEST_COUNT; idx++) {
		dlist.add_head(datas[idx]);
	}
	EXPECT_EQ(MUL_TEST_COUNT, dlist.count_);
	EXPECT_TRUE(is_dlist_correct(MUL_TEST_COUNT - 1, dlist.head()));

	// Delete the last half of the data and check for correctness
	for (int64_t idx = 0; idx < MUL_TEST_COUNT / 2; idx++) {
		dlist.erase(datas[idx]);
	}
	EXPECT_EQ(MUL_TEST_COUNT / 2, dlist.count_);
	EXPECT_TRUE(is_dlist_correct(MUL_TEST_COUNT - 1, dlist.head()));

	// Delete the first half of the data and check for correctness
	for (int64_t idx = MUL_TEST_COUNT / 2; idx < MUL_TEST_COUNT; idx++) {
		dlist.erase(datas[idx]);
	}
	EXPECT_EQ(0, dlist.count_);
}

}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_dlist.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
