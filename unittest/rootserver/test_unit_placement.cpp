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
#include "lib/utility/ob_test_util.h"
#include "lib/json/ob_json.h"
#include "rootserver/ob_unit_placement_strategy.h"
#include "ob_rs_test_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase;

class TestUnitPlacement: public ::testing::Test
{
public:
  TestUnitPlacement();
  virtual ~TestUnitPlacement();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestUnitPlacement);
  static const char* const BASE_DIR;
protected:
  // function members
  void run_case(const char* casename);
  void build_case(json::Value *root,
                  common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers,
                  common::ObArray<share::ObUnitConfig> &units);
  void output_result(const char* filename,
                     const common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers,
                     const ObArray<int64_t> &units_placement);
protected:
  // data members
};

const char* const TestUnitPlacement::BASE_DIR = "./unit_placement_testcase/";

TestUnitPlacement::TestUnitPlacement()
{
}

TestUnitPlacement::~TestUnitPlacement()
{
}

void TestUnitPlacement::SetUp()
{
}

void TestUnitPlacement::TearDown()
{
}

class CaseBuilder: public json::Walker
{
  enum State
  {
    IS_NONE,
    IS_SERVER,
    IS_ZONE,
    IS_SERVER_CPU,
    IS_SERVER_MEM,
    IS_UNIT_CPU,
    IS_UNIT_MEM,
    IS_UNIT_NUM
  };
public:
  CaseBuilder(const json::Value *root,
              common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers,
              common::ObArray<share::ObUnitConfig> &units)
      :Walker(root),
       servers_(servers),
       units_(units),
       in_servers_(false),
       in_units_(false),
       state_(IS_NONE),
       cur_unit_config_id_(1)
  {}
  virtual ~CaseBuilder() {}

  virtual int on_null(int level, json::Type parent)
  {
    UNUSED(level);
    UNUSED(parent);
    return OB_SUCCESS;
  }
  virtual int on_true(int level, json::Type parent)
  {
    UNUSED(level);
    UNUSED(parent);
    return OB_SUCCESS;
  }
  virtual int on_false(int level, json::Type parent)
  {
    UNUSED(level);
    UNUSED(parent);
    return OB_SUCCESS;
  }
  virtual int on_string(int level, json::Type parent, const json::String &str)
  {
    UNUSED(level);
    UNUSED(parent);
    if (state_ == IS_SERVER) {
      cur_server_resource_.addr_.set_ip_addr(str, 8888);
      OB_LOG(DEBUG, "addr value", K(str), "addr", cur_server_resource_.addr_);
    }
    return OB_SUCCESS;
  }
  virtual int on_number(int level, json::Type parent, const json::Number &num)
  {
    UNUSED(level);
    UNUSED(parent);
    switch(state_) {
      case IS_SERVER_CPU:
        cur_server_resource_.capacity_[RES_CPU] = static_cast<double>(num);
        break;
      case IS_SERVER_MEM:
        cur_server_resource_.capacity_[RES_MEM] = static_cast<double>(num);
        break;
      case IS_UNIT_CPU:
        cur_unit_.min_cpu_ = static_cast<double>(num);
        break;
      case IS_UNIT_MEM:
        cur_unit_.min_memory_ = num;
        break;
      case IS_UNIT_NUM:
        cur_unit_num_ = num;
      default:
        break;
    }
    return OB_SUCCESS;
  }
  virtual int on_array_start(int level, json::Type parent, const json::Array &arr)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(arr);
    return OB_SUCCESS;
  }
  virtual int on_array_end(int level, json::Type parent, const json::Array &arr)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(arr);
    return OB_SUCCESS;
  }
  virtual int on_array_item_start(int level, json::Type parent, const json::Value *val)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(val);
    return OB_SUCCESS;
  }
  virtual int on_array_item_end(int level, json::Type parent, const json::Value *val, bool is_last)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(val);
    UNUSED(is_last);
    return OB_SUCCESS;
  }
  virtual int on_object_start(int level, json::Type parent, const json::Object &obj)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(obj);
    return OB_SUCCESS;
  }
  virtual int on_object_end(int level, json::Type parent, const json::Object &obj)
  {
    int ret = OB_SUCCESS;
    UNUSED(level);
    UNUSED(obj);
    if (parent == json::JT_ARRAY
        && in_servers_) {
      ret = servers_.push_back(cur_server_resource_);
    } else if (in_units_ && parent == json::JT_ARRAY) {
      cur_unit_.unit_config_id_ = cur_unit_config_id_++;
      for (int64_t i = 0; i < cur_unit_num_ && OB_SUCC(ret); ++i) {
        ret = units_.push_back(cur_unit_);
      }
    }
    return ret;
  }
  virtual int on_object_member_start(int level, json::Type parent, const json::Pair *kv)
  {
    UNUSED(level);
    UNUSED(parent);
    if (kv->name_ == ObString::make_string("servers")) {
      servers_.reuse();
      in_servers_ = true;
    } else if (kv->name_ == ObString::make_string("units")) {
      units_.reuse();
      in_units_ = true;
    }

    state_ = IS_NONE;
    if (in_servers_) {
      if (kv->name_ == ObString::make_string("server")) {
        state_ = IS_SERVER;
      } else if (kv->name_ == ObString::make_string("zone")) {
      } else if (kv->name_ == ObString::make_string("resources")) {
      } else if (kv->name_ == ObString::make_string("cpu")) {
        state_ = IS_SERVER_CPU;
      } else if (kv->name_ == ObString::make_string("memory")) {
        state_ = IS_SERVER_MEM;
      }
    } else if (in_units_) {
      if (kv->name_ == ObString::make_string("cpu")) {
        state_ = IS_UNIT_CPU;
      } else if (kv->name_ == ObString::make_string("memory")) {
        state_ = IS_UNIT_MEM;
      } else if (kv->name_ == ObString::make_string("num")) {
        state_ = IS_UNIT_NUM;
      }
    }
    return OB_SUCCESS;
  }
  virtual int on_object_member_end(int level, json::Type parent, const json::Pair *kv, bool is_last)
  {
    UNUSED(level);
    UNUSED(parent);
    UNUSED(is_last);
    if (kv->name_ == ObString::make_string("servers")) {
      in_servers_ = false;
    } else if (kv->name_ == ObString::make_string("units")) {
      in_units_ = false;
    }

    return OB_SUCCESS;
  }
  virtual int on_walk_start()
  {
    return OB_SUCCESS;
  }
  virtual int on_walk_end()
  {
    return OB_SUCCESS;
  }
private:
  common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers_;
  common::ObArray<share::ObUnitConfig> &units_;
  ObUnitPlacementStrategy::ObServerResource cur_server_resource_;
  share::ObUnitConfig cur_unit_;
  int64_t cur_unit_num_;
  bool in_servers_;
  bool in_units_;
  State state_;
  uint64_t cur_unit_config_id_;
};

void TestUnitPlacement::build_case(json::Value *root,
                                   common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers,
                                   common::ObArray<share::ObUnitConfig> &units)
{
  CaseBuilder case_builder(root, servers, units);
  ASSERT_EQ(OB_SUCCESS, case_builder.go());
  OB_LOG(INFO, "CASE", K(servers), K(units));
}

void TestUnitPlacement::output_result(const char* filename,
                                      const common::ObArray<ObUnitPlacementStrategy::ObServerResource> &servers,
                                      const ObArray<int64_t> &units_placement)
{
  FILE *fp = fopen(filename, "w+");
  ASSERT_TRUE(NULL != fp);

  for (int64_t i = 0; i < servers.count(); ++i) {
    const ObUnitPlacementStrategy::ObServerResource &s = servers.at(i);
    fprintf(fp, "Server: %s\n", S(s.addr_));
    fprintf(fp, "CPU: %f\n", s.assigned_[RES_CPU]/s.capacity_[RES_CPU]);
    fprintf(fp, "MEM: %f\n", s.assigned_[RES_MEM]/s.capacity_[RES_MEM]);
    fprintf(fp, "UNITS: ");
    for (int64_t j = 0; j < units_placement.count(); ++j) {
      if (units_placement.at(j) == i) {
        fprintf(fp, "%ld ", j);
      }
    }
    fprintf(fp, "\n\n");
  }

  if (NULL != fp) {
    fclose(fp);
  }
}

void TestUnitPlacement::run_case(const char* casename)
{
  char filename[512];
  snprintf(filename, 512, "%s%s.test", BASE_DIR, casename);
  printf("run case: %s\n", filename);
  ObArenaAllocator allocator(ObModIds::TEST);
  json::Value *root = NULL;
  common::ObArray<ObUnitPlacementStrategy::ObServerResource> servers;
  common::ObArray<share::ObUnitConfig> units;

  ASSERT_NO_FATAL_FAILURE(ob_parse_case_file(allocator, filename, root));
  ASSERT_NO_FATAL_FAILURE(build_case(root, servers, units));

  // go
  ObUnitPlacementDPStrategy placement_s;
  common::ObAddr chosen;
  ObArray<int64_t> units_placement;
  for (int64_t i = 0; i < units.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, placement_s.choose_server(servers, units.at(i), chosen));
    OB_LOG(INFO, "server is choosen", K(i), K(chosen));
    for (int j = 0; j < servers.count(); ++j) {
      ObUnitPlacementStrategy::ObServerResource &s = servers.at(j);
      if (s.addr_ == chosen) {
        s.assigned_[RES_CPU] += units.at(i).min_cpu_;
        s.assigned_[RES_MEM] += static_cast<double>(units.at(i).min_memory_);
        ASSERT_EQ(OB_SUCCESS, units_placement.push_back(j));
        break;
      }
    }
  }

  // print result
  OB_LOG(INFO, "OUTPUT", K(servers));
  char output_file[512];
  snprintf(output_file, 512, "%s%s.tmp", BASE_DIR, casename);
  ASSERT_NO_FATAL_FAILURE(output_result(output_file, servers, units_placement));
  ASSERT_NO_FATAL_FAILURE(ob_check_result(BASE_DIR, casename));
}

TEST_F(TestUnitPlacement, basic_test)
{
  ASSERT_NO_FATAL_FAILURE(run_case("shenglian"));
  ASSERT_NO_FATAL_FAILURE(run_case("ffd_avg_sum"));
  ASSERT_NO_FATAL_FAILURE(run_case("split_demands"));
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
