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
#define USING_LOG_PREFIX COMMON
#include <stdlib.h>
#include <sys/wait.h>
#include <iterator>
#include <gtest/gtest.h>
#include "test_op_engine.h"
#include "src/observer/omt/ob_tenant_config_mgr.h"
#include "sql/test_sql_utils.h"
#include "lib/container/ob_array.h"
#include "sql/ob_sql_init.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "observer/ob_req_time_service.h"
#include "ob_fake_table_scan_vec_op.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "src/share/ob_local_device.h"
#include "src/share/ob_device_manager.h"
#include "src/storage/blocksstable/ob_storage_cache_suite.h"
#include "ob_test_config.h"
#include <vector>
#include <string>

using namespace oceanbase::sql;
namespace test
{
TestOpEngine::TestOpEngine() : tbase_{sys_tenant_id_}, vec_2_exec_ctx_(vec_2_alloc_)
{
  vec_2_exec_ctx_.set_sql_ctx(&sql_ctx_);
}

TestOpEngine::~TestOpEngine()
{
  destory();
}

void TestOpEngine::SetUp()
{
  TestOptimizerUtils::SetUp();
  addr_.set_ip_addr("1.1.1.1", 8888);
  vec_2_exec_ctx_.set_my_session(&session_info_);
  vec_2_exec_ctx_.create_physical_plan_ctx();
  ASSERT_EQ(prepare_io(ObTestOpConfig::get_instance().test_filename_prefix_), OB_SUCCESS);

  // init mock location service, used in optimizer compute table property
  GCTX.location_service_ = &mock_location_service_;
  // init MTL, used in ObTableScanOp::ObTableScanOp constructor
  static ObDataAccessService instance;
  tbase_.inner_set(&instance);
  ASSERT_EQ(tbase_.init(), 0);
  ObTenantEnv::set_tenant(&tbase_);

  out_origin_result_stream_.open(ObTestOpConfig::get_instance().test_filename_origin_output_file_, std::ios::out | std::ios::trunc);
  out_vec_result_stream_.open(ObTestOpConfig::get_instance().test_filename_vec_output_file_, std::ios::out | std::ios::trunc);
}

void TestOpEngine::TearDown()
{
  destroy();
}

void TestOpEngine::destory()
{
  OB_SERVER_BLOCK_MGR.stop();
  OB_SERVER_BLOCK_MGR.wait();
  OB_SERVER_BLOCK_MGR.destroy();
  OB_STORE_CACHE.destroy();
  ObIOManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObClusterVersion::get_instance().destroy();
  oceanbase::blocksstable::ObTmpFileManager::get_instance().destroy();

  // THE_IO_DEVICE->destroy();
}

common::ObIODevice *TestOpEngine::get_device_inner()
{
  int ret = OB_SUCCESS;
  common::ObIODevice *device = NULL;
  common::ObString storage_info(OB_LOCAL_PREFIX);
  // for the local and nfs, storage_prefix and storage info are same
  if (OB_FAIL(common::ObDeviceManager::get_instance().get_device(storage_info, storage_info, device))) {
    LOG_WARN("get_device_inner", K(ret));
  }
  return device;
}

// copy from mittest/mtlenv/mock_tenant_module_env.h and unittest/storage/blocksstable/ob_data_file_prepare.h
// refine some code
// call prepare_io() for testing operators that needs to dump intermediate data
int TestOpEngine::prepare_io(const string & test_data_name_suffix)
{
  int ret = OB_SUCCESS;

  ObIODOpt iod_opt_array[5];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;
  int64_t macro_block_count = 5 * 1024;
  int64_t macro_block_size = 64 * 1024;
  char cur_dir[OB_MAX_FILE_NAME_LENGTH];
  char test_data_name[OB_MAX_FILE_NAME_LENGTH];
  char data_dir[OB_MAX_FILE_NAME_LENGTH];
  char file_dir[OB_MAX_FILE_NAME_LENGTH];
  char clog_dir[OB_MAX_FILE_NAME_LENGTH];
  char slog_dir[OB_MAX_FILE_NAME_LENGTH];
  if (NULL == getcwd(cur_dir, OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "cannot get cur dir", K(ret));
  } else if (OB_FAIL(databuff_printf(test_data_name, OB_MAX_FILE_NAME_LENGTH, "%s", test_data_name_suffix.data()))) {
    STORAGE_LOG(WARN, "failed to gen test name", K(ret));
  } else if (OB_FAIL(databuff_printf(data_dir, OB_MAX_FILE_NAME_LENGTH, "%s/data_%s", cur_dir, test_data_name))) {
    STORAGE_LOG(WARN, "failed to gen data dir", K(ret));
  } else if (OB_FAIL(databuff_printf(file_dir, OB_MAX_FILE_NAME_LENGTH, "%s/sstable/", data_dir))) {
    STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(slog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/slog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen slog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/clog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen clog dir", K(ret));
  }
  storage_env_.data_dir_ = data_dir;
  storage_env_.sstable_dir_ = file_dir;
  storage_env_.clog_dir_ = clog_dir;
  storage_env_.default_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_size_ = macro_block_count * common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_percentage_ = 0;
  storage_env_.log_disk_size_ = 20 * 1024 * 1024 * 1024ll;
  share::ObLocalDevice *local_device = static_cast<share::ObLocalDevice *>(get_device_inner());
  THE_IO_DEVICE = local_device;
  iod_opt_array[0].set("data_dir", storage_env_.data_dir_);
  iod_opt_array[1].set("sstable_dir", storage_env_.sstable_dir_);
  iod_opt_array[2].set("block_size", storage_env_.default_block_size_);
  iod_opt_array[3].set("datafile_disk_percentage", storage_env_.data_disk_percentage_);
  iod_opt_array[4].set("datafile_size", storage_env_.data_disk_size_);
  iod_opts.opt_cnt_ = 5;
  ObTenantIOConfig io_config = ObTenantIOConfig::default_instance();
  const int64_t async_io_thread_count = 8;
  const int64_t sync_io_thread_count = 2;
  const int64_t max_io_depth = 256;
  const int64_t bucket_num = 1024L;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  char cmd[OB_MAX_FILE_NAME_LENGTH];
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(cmd, OB_MAX_FILE_NAME_LENGTH, "rm -rf %s", data_dir))) {
    LOG_WARN("failed to gen cmd", K(ret));
  } else if (0 != system(cmd)) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to exec cmd", K(ret), K(cmd), K(errno), KERRMSG);
  } else if (OB_FAIL(THE_IO_DEVICE->init(iod_opts))) {
    LOG_WARN("fail to init io device", K(ret), K_(storage_env));
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    LOG_WARN("fail to init io manager", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(THE_IO_DEVICE, async_io_thread_count,
                                                                    sync_io_thread_count, max_io_depth))) {
    LOG_WARN("add device channel failed", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    LOG_WARN("fail to start io manager", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(THE_IO_DEVICE, storage_env_.default_block_size_))) {
    STORAGE_LOG(WARN, "init block manager fail", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(file_dir))) {
    STORAGE_LOG(WARN, "failed to create file dir", K(ret), K(file_dir));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.start(0 /*reserver_size*/))) {
    STORAGE_LOG(WARN, "Fail to start server block mgr", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.first_mark_device())) {
    STORAGE_LOG(WARN, "Fail to start first mark device", K(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.init(10, 1, 1, 1, 1, 10000, 10))) {
    LOG_WARN("fail to init OB_STORE_CACHE, ", K(ret));
  } else {
  }
  FILE_MANAGER_INSTANCE_V2.init();
  return ret;
}

int TestOpEngine::do_rewrite(ObStmt *&stmt, ObPhysicalPlan *phy_plan)
{
  int ret = OB_SUCCESS;

  ObSchemaChecker schema_checker;
  if (OB_FAIL(schema_checker.init(sql_schema_guard_))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    ObTransformerCtx transformer_ctx;
    transformer_ctx.allocator_ = &allocator_;
    transformer_ctx.session_info_ = &session_info_;
    transformer_ctx.schema_checker_ = &schema_checker;
    transformer_ctx.exec_ctx_ = &exec_ctx_;
    transformer_ctx.expr_factory_ = &expr_factory_;
    transformer_ctx.stmt_factory_ = &stmt_factory_;
    // ctx.stat_mgr_ = &stat_manager_;
    transformer_ctx.sql_schema_guard_ = &sql_schema_guard_;
    transformer_ctx.self_addr_ = &addr_;
    transformer_ctx.merged_version_ = OB_MERGED_VERSION_INIT;

    transformer_ctx.phy_plan_ = phy_plan;

    ObTransformerImpl transformer(&transformer_ctx);
    ObDMLStmt *sql_stmt = dynamic_cast<ObDMLStmt *>(stmt);
    if (stmt->is_explain_stmt()) { sql_stmt = static_cast<ObExplainStmt *>(stmt)->get_explain_query_stmt(); }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transformer.transform(sql_stmt))) {
        LOG_WARN("Failed to transform statement", K(ret));
      } else {
        stmt = sql_stmt;
      }
    }
  }

  return ret;
}

int TestOpEngine::do_optimize(ObStmt *stmt, ObLogPlan *&plan, ObPhyPlanType distr, ObArenaAllocator &allocator,
                              ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *dml_stmt = dynamic_cast<ObDMLStmt *>(stmt);
  ObOptimizerContext *ctx_ptr = static_cast<ObOptimizerContext *>(allocator.alloc(sizeof(ObOptimizerContext)));
  exec_ctx.get_sql_ctx()->session_info_ = &session_info_;
  ObOptimizerContext *opt_ctx = new (ctx_ptr) ObOptimizerContext(
    &session_info_, &exec_ctx,
    // schema_mgr_, // schema manager
    &sql_schema_guard_,
    //&stat_manager_, // statistics manager
    NULL, // statistics manager
    static_cast<ObIAllocator &>(allocator_), &param_store_, addr_, NULL, dml_stmt->get_query_ctx()->get_global_hint(),
    expr_factory_, dml_stmt, false, stmt_factory_.get_query_ctx());
  opt_ctx->set_opt_stat_manager(&opt_stat_manager_);
  opt_ctx->disable_batch_rpc();
  opt_ctx->set_local_server_addr("1.1.1.1", 8888);
  opt_ctx->set_use_default_stat();
  ObTableLocation table_location;
  ret = opt_ctx->get_table_location_list().push_back(table_location);
  ObOptimizer optimizer(*opt_ctx);
  if (OB_FAIL(optimizer.optimize(*dml_stmt, plan))) { LOG_WARN("failed to optimize", "SQL", *dml_stmt); }
  return ret;
}

int TestOpEngine::do_code_generate(const ObLogPlan &log_plan, ObCodeGenerator &code_gen, ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_cluster_version = CLUSTER_CURRENT_VERSION;

  // WARN: may have bug here
  log_plan.get_optimizer_context().set_batch_size(ObTestOpConfig::get_instance().batch_size_);
  phy_plan.set_batch_size(ObTestOpConfig::get_instance().batch_size_);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(code_gen.generate_exprs(log_plan, phy_plan, cur_cluster_version))) {
    LOG_WARN("fail to get all raw exprs", K(ret));
  } else if (OB_FAIL(code_gen.generate_operators(log_plan, phy_plan, cur_cluster_version))) {
    LOG_WARN("fail to generate plan", K(ret));
  }

  return ret;
}

int TestOpEngine::test_phy_plan(ObPhysicalPlan &plan)
{
  UNUSED(plan);
  int ret = OB_SUCCESS;
  return ret;
}

ObOperator *TestOpEngine::subtitude_table_scan_to_fake(ObOperator *root)
{
  for (uint32 i = 0; i < root->get_child_cnt(); i++) {
    root->children_[i] = subtitude_table_scan_to_fake(root->children_[i]);
    if (i == 0) {
      root->left_ = root->children_[i];
      root->left_->parent_ = root;
    }
    if (i == 1) {
      root->right_ = root->children_[i];
      root->right_->parent_ = root;
    }
  }

  if (root->get_spec().get_type() == PHY_TABLE_SCAN) {
    root = new oceanbase::sql::ObFakeTableScanVecOp(root->get_exec_ctx(), root->get_spec(), root->get_input());
  }
  return root;
}

int TestOpEngine::get_tested_op_from_string(const std::string &sql, bool vector_2, ObOperator *&op,
                                            ObExecutor &executor, bool use_old_ctx)
{
  int ret = OB_SUCCESS;
  ObStmt *stmt = NULL;
  ObLogPlan *log_plan = NULL;
  ObPhysicalPlan *phy_plan = NULL;
  ObArenaAllocator *p_alloc = NULL;
  ObExecContext *p_exec_ctx = NULL;

  if (vector_2 || !use_old_ctx) {
    p_alloc = &vec_2_alloc_;
    p_exec_ctx = &vec_2_exec_ctx_;
  } else {
    p_alloc = &allocator_;
    p_exec_ctx = &exec_ctx_;
  }
  // 1.resolve
  // 2.rewrite
  // 3.optimize
  do_resolve(sql.c_str(), stmt, true, JSON_FORMAT, OB_SUCCESS, false);
  if (OB_FAIL(do_rewrite(stmt, phy_plan))) {
    LOG_ERROR("rewrite failed", K(ret));
  } else if (OB_FAIL(do_optimize(stmt, log_plan, OB_PHY_PLAN_LOCAL, *p_alloc, *p_exec_ctx))) {
    LOG_ERROR("optimize failed", K(ret));
  } else if (NULL == log_plan) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_plan is null");
  } else if (OB_FAIL(ObCacheObjectFactory::alloc(phy_plan))) {
    LOG_ERROR("fail to allocate mem to phy_plan", K(ret));
  } else if (NULL == phy_plan) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("phy_plan is null");
  } else {
    // display explain plan
    oceanbase::sql::ObExplainDisplayOpt option;
    option.with_tree_line_ = true;
    ObSqlPlan sql_plan(log_plan->get_allocator());
    ObSEArray<common::ObString, 64> plan_strs;
    if (OB_FAIL(sql_plan.print_sql_plan(log_plan, EXPLAIN_EXTENDED_NOADDR, option, plan_strs))) {
      LOG_WARN("failed to store sql plan", K(ret));
    } else {
      LOG_INFO("Generate Logical plan:");
      _OB_LOG(INFO, "%*s", plan_strs.at(0).length(), plan_strs.at(0).ptr());
    }

    // 4.generate physical plan
    if (OB_FAIL(generate_physical_plan(log_plan, *phy_plan, *p_exec_ctx, vector_2))) {
      LOG_ERROR("generate physical plan failed", K(ret));
    }

    // 5.open and get runtime op
    // ObFakeTableScanOp will replace bottom ObTableScanOp here
    if (OB_FAIL(open_and_get_op(*p_exec_ctx, executor, *phy_plan, op))) { LOG_ERROR("open operators failed", K(ret)); }
  }

  return ret;
}

std::string TestOpEngine::get_decimal_result_from_datum(ObExpr *expr, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  std::string result;
  switch (get_decimalint_type(expr->datum_meta_.precision_)) {
  case common::DECIMAL_INT_32: result = std::to_string(datum.get_decimal_int32()); break;
  case common::DECIMAL_INT_64: result = std::to_string(datum.get_decimal_int64()); break;
  case common::DECIMAL_INT_128: result = std::to_string(datum.get_decimal_int128()); break;
  case common::DECIMAL_INT_256: result = std::to_string(datum.get_decimal_int256()); break;
  case common::DECIMAL_INT_512: result = std::to_string(datum.get_decimal_int512()); break;
  default: LOG_WARN("unexpected precision", K(expr->datum_meta_));
  }
  return result;
}

std::string TestOpEngine::get_decimal_result_from_payload(ObExpr *expr, const char *payload)
{
  int ret = OB_SUCCESS;
  std::string result;
  switch (get_decimalint_type(expr->datum_meta_.precision_)) {
  case common::DECIMAL_INT_32: result = std::to_string(*(int32_t *)payload); break;
  case common::DECIMAL_INT_64: result = std::to_string(*(int64_t *)payload); break;
  case common::DECIMAL_INT_128: result = std::to_string(*(int128_t *)payload); break;
  case common::DECIMAL_INT_256: result = std::to_string(*(int256_t *)payload); break;
  case common::DECIMAL_INT_512: result = std::to_string(*(int512_t *)payload); break;
  default: LOG_WARN("unexpected precision", K(expr->datum_meta_));
  }
  return result;
}

std::string TestOpEngine::get_data_by_datum_type(const ObOperator *op, ObExpr *expr, ObEvalCtx &eval_ctx, int row)
{
  std::string result_str;
  ObIVector *i_vector = NULL;
  ObDatum *datums = NULL;
  if (expr->enable_rich_format() && op->spec_.get_type() != PHY_EXPR_VALUES) {
    i_vector = expr->get_vector(eval_ctx);
    if (i_vector->is_null(row)) { result_str = "null"; }
  } else {
    datums = expr->locate_batch_datums(eval_ctx);
    if (datums[row].is_null()) { result_str = "null"; }
  }

  if (result_str != "null") {
    switch (expr->datum_meta_.get_type()) {
    // why expr->datum_meta_.get_type() == ObInt32Type while expr->res_buf_len_ == 8 ?????
    case ObInt32Type:
      if (i_vector != NULL) {
        result_str = std::to_string(i_vector->get_int32(row));
        // str_result = std::to_string(i_vector->get_int(row));
      } else {
        result_str = std::to_string(datums[row].get_int32());
      }
      break;
    case ObIntType: {
      if (i_vector != NULL) {
        result_str = std::to_string(i_vector->get_int(row));
      } else {
        result_str = std::to_string(datums[row].get_int());
      }
      break;
    }
    case ObDoubleType: {
      if (i_vector != NULL) {
        result_str = std::to_string(i_vector->get_double(row));
      } else {
        result_str = std::to_string(datums[row].get_double());
      }
      break;
    }
    case ObNumberType: {
      // result type of avg()
      if (i_vector != NULL) {
        ObNumber ob_num(i_vector->get_number(row));
        char buf[ob_num.get_length()];
        ob_num.to_string(buf, ob_num.get_length());
        result_str = std::string(buf, ob_num.get_length());
      } else {
        ObNumber ob_num(datums[row].get_number());
        char buf[ob_num.get_length()];
        ob_num.to_string(buf, ob_num.get_length());
        result_str = std::string(buf, ob_num.get_length());
      }
      break;
    }
    case ObDecimalIntType: {
      // result type of sum()
      if (i_vector != NULL) {
        // TODO: replace this when i_vector have get_decimalXXX() api
        result_str = get_decimal_result_from_payload(expr, i_vector->get_payload(row));
      } else {
        result_str = get_decimal_result_from_datum(expr, datums[row]);
      }
      break;
    }
    case ObVarcharType:
    case ObCharType: {
      if (i_vector != NULL) {
        result_str = std::string(i_vector->get_payload(row), i_vector->get_length(row));
      } else {
        // str_result = std::to_string(datums[row].get_int32());
        result_str = std::string(datums[row].ptr_, datums[row].len_);
      }
      break;
    }
    default: LOG_INFO("Can not display value so far for: ", K(expr->datum_meta_.get_type()));
    }
  }

  return result_str;
}

int TestOpEngine::generate_physical_plan(ObLogPlan *log_plan, ObPhysicalPlan &phy_plan, ObExecContext &exec_ctx,
                                         bool enable_rich_format)
{
  int ret = OB_SUCCESS;

  // begin generate phsical plan
  ObPhysicalPlanCtx *pctx = exec_ctx.get_physical_plan_ctx();
  if (NULL == pctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pctx is null");
  } else {
    /*
    bool ObStaticEngineExprCG::enable_rich_format() const {
      //TODO shengle change the version
      return cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0
            && op_cg_ctx_.session_->enable_rich_format();
    }
    */
    // So we need set here to support rich_format
    ObCodeGenerator code_gen(false /*use_jit*/, CLUSTER_VERSION_4_3_0_0, &(pctx->get_datum_param_store()));
    log_plan->get_optimizer_context().get_session_info()->sys_vars_cache_.set_enable_rich_vector_format(enable_rich_format);
    log_plan->get_optimizer_context().get_session_info()->init_use_rich_format();
    phy_plan.set_use_rich_format(enable_rich_format);

    if (OB_FAIL(do_code_generate(*log_plan, code_gen, phy_plan))) {
      LOG_ERROR("Can not generate physical plan ", K(ret));
    } else {
      pctx->set_phy_plan(&phy_plan);
    }
  }

  return ret;
}

int TestOpEngine::open_and_get_op(ObExecContext &exec_ctx, ObExecutor &ob_exe, ObPhysicalPlan &phy_plan,
                                  ObOperator *&root)
{
  int ret = OB_SUCCESS;

  ObTaskExecutorCtx &task_exec_ctx = exec_ctx.get_task_exec_ctx();
  ObExecuteResult &exe_result = task_exec_ctx.get_execute_result();
  // ObExecutor ob_exe;
  if (OB_FAIL(ob_exe.init(&phy_plan))) {
    LOG_WARN("fail init exec ObExecutor", K(ret));
  } else if (OB_FAIL(exec_ctx.init_phy_op(phy_plan.get_phy_operator_size()))) {
    LOG_WARN("fail init exec phy op ctx", K(ret));
  } else if (OB_FAIL(exec_ctx.init_expr_op(phy_plan.get_expr_operator_size()))) {
    LOG_WARN("fail init exec expr op ctx", K(ret));
  } else if (OB_FAIL(ob_exe.execute_plan(exec_ctx))) {
    LOG_ERROR("execute plan fail ", K(ret));
  } else {
    // ob_exe.execute_plan(exec_ctx_) will use ObArenaAllocator to allocate memory for original ObTableScanOp
    // while *subtitude_table_scan_to_fake()* will directly use *new* to new a ObFakeTableScanVecOp, for simple
    exe_result.static_engine_root_ = subtitude_table_scan_to_fake(exe_result.static_engine_root_);

    if (OB_FAIL(exe_result.open(exec_ctx))) {
      LOG_ERROR("open plan fail ", K(ret));
    } else {
      root = exe_result.static_engine_root_;
    }
  }

  return ret;
}

int TestOpEngine::print_and_cmp_final_output(const ObBatchRows *brs, ObOperator *root, bool is_comparing)
{
  int ret = OB_SUCCESS;
  std::string output_line;
  for (int i = 0; i < brs->size_; i++) {
    if (!brs->skip_->exist(i)) {
      for (int j = 0; j < root->get_spec().output_.count(); j++) {
        ObExpr *output_expr = root->get_spec().output_.at(j);
        string result_str = get_data_by_datum_type(root, output_expr, root->eval_ctx_, i);
        output_line += result_str + " ";

        // compare whether is the same output
        // cmp output results
        if (is_comparing) {
          if (result_str.compare(temp_cmp_data_[i][j]) != 0) {
            // different
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Got an different output result in :", K(i), K(j));
            LOG_WARN("Original op output value is :", K(ObString(temp_cmp_data_[i][j].data())));
            LOG_WARN("     New op output value is :", K(ObString(result_str.data())));
          }
        } else {
          temp_cmp_data_[i].push_back(result_str);
        }
      }
      LOG_INFO(output_line.data());
      output_line.clear();
    }
  }
  return ret;
}

//
int TestOpEngine::print_to_file(const ObBatchRows *brs, ObOperator *root, const ExprFixedArray &exprs, bool is_result,
                                std::ofstream *out_data_stream)
{
  int ret = OB_SUCCESS;

  std::string output_line;
  for (int i = 0; i < brs->size_; i++) {
    if (!brs->skip_->exist(i)) {
      for (int j = 0; j < exprs.count(); j++) {
        ObExpr *expr = exprs.at(j);
        string result_str = get_data_by_datum_type(root, expr, root->eval_ctx_, i);
        output_line += result_str;
        if (j != exprs.count() - 1) { output_line += ", "; }
      }
      if (is_result) {
        *out_data_stream << output_line << std::endl;
      } else {
        *out_data_stream << "insert into " << reinterpret_cast<ObFakeTableScanVecOp *>(root)->get_tsc_spec().table_name_
                         << " values(" << output_line << ");" << std::endl;
      }

      output_line.clear();
    }
  }
  return ret;
}

int TestOpEngine::basic_random_test(const std::string &test_file)
{
  // run tests
  std::ifstream if_tests(test_file);
  if (if_tests.is_open() != true) { return -1; }

  int ret = OB_SUCCESS;
  std::string line;

  while (std::getline(if_tests, line)) {
    // handle query
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;

    if (ObTestOpConfig::get_instance().output_result_to_file_) {
      system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log").data());
      system(("rm -f " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".log.*").data());
      system(("cat /dev/null > " + ObTestOpConfig::get_instance().test_filename_origin_output_file_).c_str());
      system(("cat /dev/null > " + ObTestOpConfig::get_instance().test_filename_vec_output_file_).c_str());
    }

    ret = OB_SUCCESS;
    ObOperator *original_root = NULL;
    ObOperator *vec_2_root = NULL;
    ObExecutor original_exector;
    ObExecutor vec_2_exector;
    if (OB_FAIL(get_tested_op_from_string(line, false, original_root, original_exector))) {
      LOG_WARN("generate original tested op fail, sql: ", K(line.data()));
    } else if (OB_FAIL(get_tested_op_from_string(line, true, vec_2_root, vec_2_exector))) {
      LOG_WARN("generate vectorization 2.0 tested op fail, sql: ", K(line.data()));
    } else {
      // begin get_next_batch()
      // 5.compare two op outputs
      int round = 1;
      const int64_t max_row_cnt = 256;
      const ObBatchRows *original_child_brs = nullptr;
      const ObBatchRows *vec_2_child_brs = nullptr;

      LOG_INFO("============== Final output ===============", K(round));
      while (!original_root->brs_.end_ || !vec_2_root->brs_.end_) {
        if (OB_FAIL(original_root->get_next_batch(max_row_cnt, original_child_brs))) {
          LOG_ERROR("root op fail to get_next_batch data", K(original_root));
          break;
        }

        temp_cmp_data_.resize(original_child_brs->size_);
        LOG_INFO("============== Original ===============", K(round));
        if (ObTestOpConfig::get_instance().output_result_to_file_) {
          if (OB_FAIL(print_to_file(original_child_brs, original_root, original_root->spec_.output_, true,
                                    &out_origin_result_stream_))) {
            // break, other error log has already print in inner function
            break;
          }
        } else {
          if (OB_FAIL(print_and_cmp_final_output(original_child_brs, original_root, false))) {
            // break, other error log has already print in inner function
            break;
          }
        }

        if (OB_FAIL(vec_2_root->get_next_batch(max_row_cnt, vec_2_child_brs))) {
          LOG_ERROR("root op fail to get_next_batch data", K(vec_2_root));
          break;
        }

        LOG_INFO("============== Vectorization 2.0 ===============", K(round));
        if (ObTestOpConfig::get_instance().output_result_to_file_) {
          if (OB_FAIL(
                print_to_file(vec_2_child_brs, vec_2_root, vec_2_root->spec_.output_, true, &out_vec_result_stream_))) {
            // break, other error log has already print in inner function
            break;
          }
        } else {
          if (OB_FAIL(print_and_cmp_final_output(vec_2_child_brs, vec_2_root, true))) {
            // break, other error log has already print in inner function
            break;
          }
        }

        if (!ObTestOpConfig::get_instance().output_result_to_file_) {
          if (original_child_brs->size_ != vec_2_child_brs->size_) {
            LOG_ERROR("Two operator output different [batch size] in", K(round), K(original_child_brs->size_),
                      K(vec_2_child_brs->size_));
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          if (MEMCMP(original_child_brs->skip_->data_, vec_2_child_brs->skip_->data_,
                     original_child_brs->skip_->memory_size(original_child_brs->size_))
              != 0) {
            LOG_ERROR("Two operator output different [skip size] in", K(round), K(original_child_brs->size_),
                      K(vec_2_child_brs->size_));
            ret = OB_ERR_UNEXPECTED;
            break;
          }
        }

        round++;
        temp_cmp_data_.clear();
      }

      //if output to file, compare data in file at last
      if (ObTestOpConfig::get_instance().output_result_to_file_) {
        if (original_root->get_spec().get_type() == PHY_HASH_JOIN && original_root->get_spec().get_type() == PHY_VEC_HASH_JOIN) {
          system(("sort " + ObTestOpConfig::get_instance().test_filename_origin_output_file_ + " -o "
                  + ObTestOpConfig::get_instance().test_filename_origin_output_file_)
                   .c_str());
          system(("sort " + ObTestOpConfig::get_instance().test_filename_vec_output_file_ + " -o "
                  + ObTestOpConfig::get_instance().test_filename_vec_output_file_)
                   .c_str());
        }

        int system_ret = system(("diff " + ObTestOpConfig::get_instance().test_filename_origin_output_file_ + " " + ObTestOpConfig::get_instance().test_filename_vec_output_file_ + " > /dev/null").c_str());
        LOG_ERROR("CODE: ", K(system_ret));
        LOG_ERROR("CODE: ", K(WEXITSTATUS(system_ret)));

        if (WEXITSTATUS(system_ret) != 0) {
          LOG_ERROR("Two operator output different!");
          // Preserve the site

          uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

          std::fstream error_output_stream;
          std::string error_file_name = "mismatch." + std::to_string(ms);
          LOG_ERROR("error_file_name: ", K(error_file_name.data()));
          error_output_stream.open(error_file_name, std::fstream::in | std::fstream::out | std::fstream::app);

          error_output_stream << line << std::endl;
          system(("cat " + ObTestOpConfig::get_instance().test_filename_prefix_ + ".cfg" + " >> " + error_file_name)
                   .c_str());
          system(("mv " + ObTestOpConfig::get_instance().test_filename_origin_output_file_ + " "
                  + ObTestOpConfig::get_instance().test_filename_origin_output_file_ + std::to_string(ms))
                   .c_str());
          system(("mv " + ObTestOpConfig::get_instance().test_filename_vec_output_file_ + " "
                  + ObTestOpConfig::get_instance().test_filename_vec_output_file_ + std::to_string(ms))
                   .c_str());
        }
      }
    }

    exec_ctx_.~ObExecContext();
    new (&exec_ctx_) ObExecContext(allocator_);
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    exec_ctx_.set_my_session(&session_info_);
    exec_ctx_.create_physical_plan_ctx();

    vec_2_exec_ctx_.~ObExecContext();
    new (&vec_2_exec_ctx_) ObExecContext(allocator_);
    vec_2_exec_ctx_.set_sql_ctx(&sql_ctx_);
    vec_2_exec_ctx_.set_my_session(&session_info_);
    vec_2_exec_ctx_.create_physical_plan_ctx();
  }

  EXPECT_EQ(ret, 0);
  if (ret == OB_SUCCESS) {
    LOG_INFO(" ======================= ");
    LOG_INFO("Test Pass!");
    LOG_INFO("All new operator output is equal to the original one.");
    LOG_INFO(" ======================= ");
  } else {
    LOG_INFO(" ======================= ");
    LOG_INFO("Test Fail!");
    LOG_INFO(" ======================= ");
  }
  return ret;
}

int TestOpEngine::basic_random_test_output_to_file(const std::string &test_file, bool vector_2)
{
  // run tests
  std::ifstream if_tests(test_file);
  if (if_tests.is_open() != true) { return -1; }

  int ret = OB_SUCCESS;
  std::string line;

  while (std::getline(if_tests, line)) {
    // handle query
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;

    ObOperator *root = NULL;
    ObExecutor exector;
    if (OB_FAIL(get_tested_op_from_string(line, vector_2, root, exector))) {
      LOG_WARN("generate tested op fail, sql: ", K(line.data()));
    } else {
      int round = 1;
      const int64_t max_row_cnt = 256;
      const ObBatchRows *child_brs = nullptr;

      LOG_INFO("============== Final output ===============", K(round));
      while (!root->brs_.end_) {
        if (OB_FAIL(root->get_next_batch(max_row_cnt, child_brs))) {
          LOG_ERROR("root op fail to get_next_batch data", K(root));
          break;
        }

        if (OB_FAIL(print_to_file(child_brs, root, root->spec_.output_, true,
                                  vector_2 ? &out_vec_result_stream_ : &out_origin_result_stream_))) {
          // break, other error log has already print in inner function
          break;
        }

        round++;
      }
    }

    exec_ctx_.~ObExecContext();
    new (&exec_ctx_) ObExecContext(allocator_);
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    exec_ctx_.set_my_session(&session_info_);
    exec_ctx_.create_physical_plan_ctx();
  }

  EXPECT_EQ(ret, 0);

  return ret;
}

} // namespace test
