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
#include <gmock/gmock.h>

#include "all_mock.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_token_calcer.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_cgroup_ctrl.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

static constexpr auto TIMES_OF_WORKERS = 10;

// Fake ObTenant class
class MockTenant : public ObTenant {
public:
  MockTenant(const int64_t id, ObWorkerPool& worker_pool) : ObTenant(id, TIMES_OF_WORKERS, worker_pool, ctrl_)
  {
    stopped_ = false;
    EXPECT_CALL(*this, waiting_count()).WillRepeatedly(::testing::Return(0));
  }
  virtual ~MockTenant()
  {}

  MOCK_CONST_METHOD0(waiting_count, int64_t());

private:
  ObCgroupCtrl ctrl_;
};

// Fake MultiTenant class, Mocked function:
//
//   get_tenant_list()
//   add_tenant()
class MockOMT : public ObMultiTenant {
public:
  MockOMT() : ObMultiTenant(procor_)
  {}

  int add_tenant(uint64_t id, double min_cpu, double max_cpu)
  {
    auto t = new MockTenant(id, worker_pool_);
    t->set_unit_min_cpu(min_cpu);
    t->set_unit_max_cpu(max_cpu);
    return tenants_.push_back(t);
  }
  void clear()
  {
    std::for_each(tenants_.begin(), tenants_.end(), [](ObTenant*& t) { delete t; });
    tenants_.reset();
  }

private:
  ObFakeWorkerProcessor procor_;
};

class TestTokenCalcer : public ::testing::Test {
public:
  TestTokenCalcer() : otc_(omt_)
  {
    all_mock_init();
  }

  virtual void SetUp()
  {
    static constexpr auto NODE_QUOTA = 10;
    EXPECT_EQ(OB_SUCCESS, omt_.init(ObAddr(), NODE_QUOTA));
  }

  virtual void TearDown()
  {
    omt_.clear();
    omt_.destroy();
  }

  void calc()
  {
    otc_.calculate();
  }
  void clear()
  {
    omt_.clear();
  }

protected:
  ObTenant* t1_;
  ObTenant* t2_;
  ObTenant* t3_;
  MockOMT omt_;
  ObTokenCalcer otc_;
};

//// Rule Min:
//
// (1) Tenant would get at least tokens of integral part of min CPU
//     slice.
//
// (2) The fractional part of tenant min CPU slice should be
//     accumulated but less than at most one token. When Tenant has
//     waiting task, it will be used.
//
TEST_F(TestTokenCalcer, RuleMin)
{
  ASSERT_EQ(10, omt_.get_node_quota());
  auto& tenants{omt_.get_tenant_list()};
  auto ID{1};

  // Number of tokens no more than (9.9999*2)
  ASSERT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 9.9999, 10));
  const auto& t{tenants[0]};
  for (auto i = 0; i < 100; i++) {
    calc();
    ASSERT_LE(19, t->sug_token_cnt());
  }
  clear();

  for (auto i = 0; i < 10; i++) {
    ASSERT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0.4999, 0.4999));
  }
  calc();
  for (auto i = 0; i < 10; i++) {
    ASSERT_EQ(0, tenants[i]->sug_token_cnt());
  }
  calc();
  for (auto i = 0; i < 10; i++) {
    // According to RuleMax1 and RuleMax2
    ASSERT_EQ(1, tenants[i]->sug_token_cnt());
  }
}

//// Rule Max:
//
// (1) The Number of Tenant tokens won't greater than its max tokens
//     count which is ceiling of tenant CPU
//     slice(quota*concurrency). e.g. Given tenant's max CPU quota is
//     1.6 and concurrency is 2, tenant's max slice is 3.2 and tenant
//     won't get tokens greater than 4, ceil(3.2).
//
// (2) Tenant max CPU slice's fractional part would be accumulated and
//     consumed whenever the accumulation is enough to compose one token.
//
TEST_F(TestTokenCalcer, RuleMax)
{
  ASSERT_EQ(10, omt_.get_node_quota());
  auto& tenants{omt_.get_tenant_list()};
  auto ID{1};

  {
    // max CPU quota: 0.3
    // max CPU slice: 0.6
    ASSERT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0.1, 0.3));
    const auto& t{tenants[0]};
    calc();
    ASSERT_EQ(0, t->sug_token_cnt());  // 0.6
    calc();
    ASSERT_EQ(1, t->sug_token_cnt());  // 1.2 - 1.0 = 0.2
    calc();
    ASSERT_EQ(0, t->sug_token_cnt());  // 0.8
    calc();
    ASSERT_EQ(1, t->sug_token_cnt());  // 1.4 - 1.0 = 0.4
    // NOTE: Following code won't generate a token because above
    //       calculation lose some precision. It's right although it is
    //       not by design.
    calc();
    ASSERT_EQ(0, t->sug_token_cnt());  // 1.0(-)
    calc();
    ASSERT_EQ(1, t->sug_token_cnt());  // 1.6(-) - 1.0 = 0.6(-)
    calc();
    ASSERT_EQ(1, t->sug_token_cnt());  // 1.2(-) - 1.0 = 0.2(-)
    calc();
    ASSERT_EQ(0, t->sug_token_cnt());  // 0.8(-)
    calc();
    ASSERT_EQ(1, t->sug_token_cnt());  // 1.4(-) - 1.0 = 0.4(-)
    calc();
    ASSERT_EQ(0, t->sug_token_cnt());  // 1.0(-)
  }
  {
    // max CPU quota: 1.4
    // max CPU slice: 2.8
    EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0.1, 1.4));
    const auto& t{tenants[1]};
    calc();
    ASSERT_EQ(2, t->sug_token_cnt());  // 2.8 - 2.0 = 0.8
    calc();
    ASSERT_EQ(3, t->sug_token_cnt());  // 3.6 - 3.0 = 0.6
    calc();
    ASSERT_EQ(3, t->sug_token_cnt());  // 3.4 - 3.0 = 0.4
    calc();
    ASSERT_EQ(3, t->sug_token_cnt());  // 3.2 - 3.0 = 0.2
    calc();
    ASSERT_EQ(2, t->sug_token_cnt());  // 3.0(-) - 2.0 = 1.0(-)
    calc();
    ASSERT_EQ(3, t->sug_token_cnt());  // 3.8(-) - 3.0 = 0.8(-)
  }
}

// Rule Normal Sell
//
// (1) If sum of all tenants max tokens is less than or equal to node
//     tokens, every tenant will get tokens according to its max
//     tokens according to RuleMax.
//
TEST_F(TestTokenCalcer, NormalSell)
{
  ASSERT_EQ(10, omt_.get_node_quota());
  auto& tenants = omt_.get_tenant_list();

  {
    // Create a tenant with 1 max CPU and 0.1 min CPU, it would get 1
    // CPU i.e. 2 tokens.
    auto ID = 1;
    EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID, 0, 10));
    calc();
    EXPECT_EQ(20, tenants[0]->sug_token_cnt());
    clear();
  }
  {
    // Create another 10 tenants with 1 max CPU, each would get 1 CPU
    // i.e. 2 tokens.
    auto ID = 1;
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, 1));
    }
    calc();
    for_each(tenants.begin(), tenants.end(), [](ObTenant*& t) { EXPECT_EQ(2, t->sug_token_cnt()); });
    clear();
  }
  {
    // Create 20 tenants with 0.5 max CPU, each would get 1 token.
    auto ID = 1;
    for (int i = 0; i < 20; i++) {
      EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, .5));
    }
    calc();
    for_each(tenants.begin(), tenants.end(), [](ObTenant*& t) { EXPECT_EQ(1, t->sug_token_cnt()); });
    clear();
  }
  {
    // Create 10 tenants with 0.5 max CPU, and one with 5 max CPU.
    auto ID = 1;
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, .5));
    }
    EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, 5));
    for (int i = 0; i < 100; i++) {  // iterate N times
      calc();
      for_each(  // All tenants but the last have 1 token.
          tenants.begin(),
          tenants.end() - 1,
          [](ObTenant*& t) { EXPECT_EQ(1, t->sug_token_cnt()); });
      for_each(  // Last tenant have 10 tokens.
          tenants.end() - 1,
          tenants.end(),
          [](ObTenant*& t) { EXPECT_EQ(10, t->sug_token_cnt()); });
    }
  }
}

//// Rule Over Sell
//
// (1) If sum of all tenants max tokens is greater than node tokens,
//     tenants would get tokens according to corresponding number of
//     waiting tasks. In this case, tenant with more waiting tasks
//     would get more tokens than that with less waiting tasks but
//     also need obey max_tokens limitation. Tenants will share node
//     tokens equally in a particully case that all tenants have no
//     waiting tasks.
//
TEST_F(TestTokenCalcer, OverSell)
{
  ASSERT_EQ(10, omt_.get_node_quota());
  auto& tenants = omt_.get_tenant_list();

  {
    // Create 10 tenants with 2 max CPU, because there is only 10 CPU,
    // so each would get 1(2 tokens).
    auto ID = 1;
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, 2));
    }
    calc();
    for_each(tenants.begin(), tenants.end(), [](ObTenant*& t) { EXPECT_EQ(2, t->sug_token_cnt()); });
    clear();
  }
  {
    // Tenants specification:
    // (min,max) => (0,1),(0,2),(0,3),(0,4),(0,5),(0,6),(0,7),(0,8),(0,9)
    auto ID = 1;
    for (int i = 0; i < 10; i++) {
      EXPECT_EQ(OB_SUCCESS, omt_.add_tenant(ID++, 0, i + 1));
    }

    // 1) If all of 10 tenants don't have tasks, they'll share node
    //    tokens equally.
    calc();
    for_each(tenants.begin(), tenants.end(), [](ObTenant*& t) {
      EXPECT_EQ(2, t->sug_token_cnt()) << "max: " << t->unit_max_cpu();
    });

    // 2) If all of tenants have only one waiting task, It's same as
    //    previous condition because previous equally assigning if
    //    "enough" for every tenant, each with two token can handle
    //    the ONLY ONE task.
    for (int i = 0; i < 10; i++) {
      auto& t = *reinterpret_cast<MockTenant*>(tenants[i]);
      // ::testing::Mock::VerifyAndClearExpectations(&t);
      EXPECT_CALL(t, waiting_count()).WillRepeatedly(::testing::Return(1));
      ASSERT_EQ(1, t.waiting_count());
    }
    calc();
    for_each(tenants.begin(), tenants.end(), [](ObTenant*& t) {
      EXPECT_EQ(2, t->sug_token_cnt()) << "max: " << t->unit_max_cpu();
    });

    // 3) If each tenant has different number of waiting tasks, how
    //    many tokens will each tenant would get is depend on its MAX
    //    QUOTA and number of waiting tasks. The more a tenant has MAX
    //    QUOTA or more waiting tasks, more tokens will the tenant
    //    got.
    for (int i = 0; i < 10; i++) {
      EXPECT_CALL(*reinterpret_cast<MockTenant*>(tenants[i]), waiting_count()).WillRepeatedly(::testing::Return(i));
      ASSERT_EQ(i, tenants[i]->waiting_count());
    }
    calc();
    for (int i = 0; i < 10; i++) {
      auto& t = tenants[i];
      // It depends but it's the result with this waiting tasks
      // distribution.
      EXPECT_EQ(i / 2, t->sug_token_cnt());
    }

    // 4) Same as 3) but given tenants have many waiting tasks, tokens
    //    distribution would obey tenant's MAX QUOTA specification.
    for (int i = 0; i < 10; i++) {
      auto& t = *reinterpret_cast<MockTenant*>(tenants[i]);
      // ::testing::Mock::VerifyAndClearExpectations(&t);
      EXPECT_CALL(t, waiting_count()).WillRepeatedly(::testing::Return(100));
      ASSERT_EQ(100, t.waiting_count());
    }
    calc();
    auto p = adjacent_find(  // Search left tenant has tokens greater
                             // than right. It should be none.
        tenants.begin(),
        tenants.end(),
        [](ObTenant* t1, ObTenant* t2) -> bool { return t1->sug_token_cnt() > t2->sug_token_cnt(); });
    EXPECT_EQ(tenants.end(), p);
    clear();
  }
}

TEST_F(TestTokenCalcer, Misc)
{
  // If no tenant exist.
  calc();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleMock(&argc, argv);
  if (argc > 1) {
    OB_LOGGER.set_log_level(3);
  }
  return RUN_ALL_TESTS();
}
