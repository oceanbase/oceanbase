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

// #define USING_LOG_PREFIX SQL_ENGINE
#include <gtest/gtest.h>
#include <fstream>
#include "../optimizer/test_optimizer_utils.h"
#include "sql/code_generator/ob_code_generator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
namespace test
{
class MockLocationService : public share::ObLocationService
{
public:
  MockLocationService()
  {}
  virtual ~MockLocationService()
  {}

  virtual int nonblock_get(const uint64_t tenant_id, const ObTabletID &tablet_id, ObLSID &ls_id)
  {
    ls_id = ObLSID::SYS_LS_ID;
    return OB_SUCCESS;
  }

  virtual int nonblock_get(const int64_t cluster_id, const uint64_t tenant_id, const ObLSID &ls_id,
                           ObLSLocation &location)
  {
    int ret = OB_SUCCESS;

    ObAddr add;
    ObReplicaProperty relica_pro;
    ObLSRestoreStatus ls_restore_sta;
    add.set_ip_addr("1.1.1.1", 8888);
    ObLSReplicaLocation ls_replica_loc;

    if (OB_FAIL(location.init(1, 1, ls_id, 1))) {
    } else if (OB_FAIL(ls_replica_loc.init(add, LEADER, 100, REPLICA_TYPE_FULL, relica_pro, ls_restore_sta, 100))) {
    } else {
      location.add_replica_location(ls_replica_loc);
    }
    return ret;
  }
};
class TestOpEngine : public TestOptimizerUtils
{
public:
  TestOpEngine();
  virtual ~TestOpEngine();
  virtual void SetUp();
  virtual void TearDown();
  virtual void destory();

  int basic_random_test(const std::string &line);
  int basic_random_test_output_to_file(const std::string &test_file, bool vector_2);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestOpEngine);

protected:
  // function members
  common::ObIODevice *get_device_inner();
  int prepare_io(const std::string & test_data_name_suffix);

  int do_optimize(ObStmt *stmt, ObLogPlan *&plan, ObPhyPlanType distr, ObArenaAllocator &allocator,
                  ObExecContext &exec_ctx);
  int do_code_generate(const ObLogPlan &log_plan, ObCodeGenerator &code_gen, ObPhysicalPlan &phy_plan);
  int do_rewrite(ObStmt *&stmt, ObPhysicalPlan *phy_plan);
  int test_phy_plan(ObPhysicalPlan &plan);
  ObOperator *subtitude_table_scan_to_fake(ObOperator *root);

  int get_tested_op_from_string(const std::string &sql, bool vector_2, ObOperator *&op,
                                ObExecutor &executor, bool use_old_ctx = false);
  int generate_physical_plan(ObLogPlan *log_plan, ObPhysicalPlan &phy_plan, ObExecContext &exec_ctx,
                             bool enable_rich_format);
  int open_and_get_op(ObExecContext &exec_ctx, ObExecutor &ob_exe, ObPhysicalPlan &phy_plan, ObOperator *&root);

  int print_and_cmp_final_output(const ObBatchRows *brs, ObOperator *root, bool is_comparing);

  static std::string get_decimal_result_from_datum(ObExpr *expr, const ObDatum &datum);
  static std::string get_decimal_result_from_payload(ObExpr *expr, const char *payload);
  static std::string get_data_by_datum_type(const ObOperator *op, ObExpr *expr, ObEvalCtx &eval_ctx, int row);
  static int print_to_file(const ObBatchRows *brs, ObOperator *root, const ExprFixedArray &exprs, bool is_result,
                           std::ofstream *out_data_stream);

protected:
  // data members
  std::vector<std::vector<std::string>> temp_cmp_data_;

  ObTenantBase tbase_;
  MockLocationService mock_location_service_;
  //
  std::string test_config_file_;
  std::string env_dir_;
  blocksstable::ObStorageEnv storage_env_;

  std::ofstream out_origin_result_stream_;
  std::ofstream out_vec_result_stream_;

  ParamStore param_store_;
  ObAddr addr_;
  ObArenaAllocator vec_2_alloc_;
  ObExecContext vec_2_exec_ctx_; // vec_2_exec_ctx_ for vectorization 2.0, there is a exec_ctx_ in father class which is
                                 // used in vectorization 1.0
};
} // namespace test
