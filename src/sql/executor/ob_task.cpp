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

#define USING_LOG_PREFIX SQL_EXE

#include "lib/utility/serialization.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/recursive_cte/ob_fake_cte_table.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all.h"
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/px/ob_px_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/json/ob_yson.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObTask::ObTask()
    : exec_ctx_(NULL),
      ser_phy_plan_(NULL),
      des_phy_plan_(NULL),
      op_root_(NULL),
      root_spec_(NULL),
      job_(NULL),
      runner_svr_(),
      ctrl_svr_(),
      ob_task_id_(),
      location_idx_(OB_INVALID_INDEX),
      partition_keys_(ObModIds::OB_SQL_EXECUTOR_TASK_PART_KEYS, OB_MALLOC_NORMAL_BLOCK_SIZE),
      max_sql_no_(-1)
{}

ObTask::~ObTask()
{}

OB_DEF_SERIALIZE(ObTask)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K_(op_root), K_(exec_ctx), K_(ser_phy_plan));
  } else if (ser_phy_plan_->is_dist_insert_or_replace_plan() && location_idx_ != OB_INVALID_INDEX) {
    OZ(exec_ctx_->reset_one_row_id_list(exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_)));
  }
  LST_DO_CODE(OB_UNIS_ENCODE, *ser_phy_plan_);
  LST_DO_CODE(OB_UNIS_ENCODE, *exec_ctx_);
  LST_DO_CODE(OB_UNIS_ENCODE, ctrl_svr_);
  LST_DO_CODE(OB_UNIS_ENCODE, runner_svr_);
  LST_DO_CODE(OB_UNIS_ENCODE, ob_task_id_);

  if (OB_SUCC(ret)) {
    if (ser_phy_plan_->is_new_engine()) {
      const ObExprFrameInfo* frame_info = &ser_phy_plan_->get_expr_frame_info();
      if (OB_ISNULL(root_spec_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: op root is null", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::serialize_expr_frame_info(
                     buf, buf_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo*>(frame_info)))) {
        LOG_WARN("failed to serialize rt expr", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::serialize_tree(buf, buf_len, pos, *root_spec_, false /**is full tree*/))) {
        LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
                     buf, buf_len, pos, *root_spec_, exec_ctx_->get_kit_store(), false /*is full tree*/))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      }
    } else {
      if (OB_ISNULL(op_root_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("task op_root is null", K(ret));
      } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *op_root_))) {
        LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
      }
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, partition_keys_);
  LST_DO_CODE(OB_UNIS_ENCODE, ranges_);
  LST_DO_CODE(OB_UNIS_ENCODE, max_sql_no_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTask)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(des_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), K_(exec_ctx), K_(des_phy_plan));
  } else {
    op_root_ = NULL;
    root_spec_ = NULL;
  }

  LST_DO_CODE(OB_UNIS_DECODE, *des_phy_plan_);
  LST_DO_CODE(OB_UNIS_DECODE, *exec_ctx_);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(exec_ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      exec_ctx_->get_my_session()->set_cur_phy_plan(des_phy_plan_);
    }
  }
  if (OB_SUCC(ret)) {
    // Compact mode may not set while rpc argument deserialize, set it manually.
    share::CompatModeGuard g(ORACLE_MODE == exec_ctx_->get_my_session()->get_compatibility_mode()
                                 ? share::ObWorker::CompatMode::ORACLE
                                 : share::ObWorker::CompatMode::MYSQL);

    LST_DO_CODE(OB_UNIS_DECODE, ctrl_svr_);
    LST_DO_CODE(OB_UNIS_DECODE, runner_svr_);
    LST_DO_CODE(OB_UNIS_DECODE, ob_task_id_);

    if (OB_SUCC(ret)) {
      if (des_phy_plan_->is_new_engine()) {
        const ObExprFrameInfo* frame_info = &des_phy_plan_->get_expr_frame_info();
        if (OB_FAIL(ObPxTreeSerializer::deserialize_expr_frame_info(
                buf, data_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo*>(frame_info)))) {
          LOG_WARN("failed to serialize rt expr", K(ret));
        } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(buf, data_len, pos, *des_phy_plan_, root_spec_))) {
          LOG_WARN("fail deserialize tree", K(ret));
        } else if (OB_FAIL(root_spec_->create_op_input(*exec_ctx_))) {
          LOG_WARN("create operator from spec failed", K(ret));
        } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(buf, data_len, pos, exec_ctx_->get_kit_store()))) {
          LOG_WARN("failed to deserialize kit store", K(ret));
        } else {
          des_phy_plan_->set_root_op_spec(root_spec_);
          GET_PHY_PLAN_CTX(*exec_ctx_)->set_phy_plan(des_phy_plan_);
        }
      } else {
        if (OB_FAIL(deserialize_tree(buf, data_len, pos, *des_phy_plan_, op_root_))) {
          LOG_WARN("fail deserialize tree", K(ret));
        } else {
          ObSEArray<ObPhyOperator*, 4> cte_pumps;
          if (OB_FAIL(build_cte_op_pair(op_root_, cte_pumps))) {
            LOG_WARN("failed to build cte pair", K(ret));
          } else if (0 != cte_pumps.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("this plan is invalid to build cte op pair", K(ret));
          } else {
            des_phy_plan_->set_main_query(op_root_);
            GET_PHY_PLAN_CTX(*exec_ctx_)->set_phy_plan(des_phy_plan_);
          }
        }
      }
    }

    LST_DO_CODE(OB_UNIS_DECODE, partition_keys_);
    LST_DO_CODE(OB_UNIS_DECODE, ranges_);

    if (OB_FAIL(ret) && OB_NOT_NULL(exec_ctx_)) {
      ObDesExecContext* des_exec_ctx = static_cast<ObDesExecContext*>(exec_ctx_);
      des_exec_ctx->cleanup_session();
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, max_sql_no_);
  if (OB_NOT_NULL(exec_ctx_) && OB_NOT_NULL(exec_ctx_->get_my_session()) &&
      max_sql_no_ > 0 /*exec_ctx_->get_my_session()->get_trans_desc().get_max_sql_no()*/) {
    exec_ctx_->get_my_session()->get_trans_desc().set_max_sql_no(max_sql_no_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTask)
{
  int64_t len = 0;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    LOG_ERROR("task not init", K_(exec_ctx), K_(ser_phy_plan));
  } else {
    if (ser_phy_plan_->is_dist_insert_or_replace_plan() && location_idx_ != OB_INVALID_INDEX) {
      exec_ctx_->reset_one_row_id_list(exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_));
    }
    LST_DO_CODE(OB_UNIS_ADD_LEN, *ser_phy_plan_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, *exec_ctx_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, ctrl_svr_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, runner_svr_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, ob_task_id_);
    if (ser_phy_plan_->is_new_engine()) {
      if (OB_ISNULL(root_spec_)) {
        LOG_ERROR("unexpected status: op root is null");
      } else {
        const ObExprFrameInfo* frame_info = &ser_phy_plan_->get_expr_frame_info();
        len += ObPxTreeSerializer::get_serialize_expr_frame_info_size(
            *exec_ctx_, *const_cast<ObExprFrameInfo*>(frame_info));
        len += ObPxTreeSerializer::get_tree_serialize_size(*root_spec_, false /*is fulltree*/);
        len += ObPxTreeSerializer::get_serialize_op_input_size(
            *root_spec_, exec_ctx_->get_kit_store(), false /*is fulltree*/);
      }
      LOG_TRACE("trace get ser rpc init sqc args size", K(len));
    } else {
      if (OB_ISNULL(op_root_)) {
        LOG_ERROR("task op_root is null", K(op_root_));
      } else {
        len += get_tree_serialize_size(*op_root_);
      }
    }
    LST_DO_CODE(OB_UNIS_ADD_LEN, partition_keys_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, ranges_);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, max_sql_no_);
  return len;
}

/*
 *          [recursive union all(1)]
 *                    |
 *        ------------------------
 *        |                      |
 *    [subquery]                [join]
 *                               |
 *                        ------------------
 *                        |                |
 *                     [pump(2)]        [subquery]
 *
 * */
int ObTask::build_cte_op_pair(ObPhyOperator* op, common::ObIArray<ObPhyOperator*>& cte_pumps)
{
  int ret = OB_SUCCESS;
  int32_t num = 0;
  ObPhyOperator* last_cte_table = NULL;
  if (OB_ISNULL(op)) {
    /*
     * create table t_refered(a int, aa bigint, b int unsigned, bb bigint unsigned, c datetime, d date, e timestamp,
     * primary key (a))  partition by hash(a) partitions 3; create table t_h5_int(a int, b int, c datetime, primary key
     * (a)) partition by hash(a) partitions 5; select * from t_h5_int t1, t_refered as t2 where t1.a =   t2.aa; After
     * the plan is demolished, there may be a son but it is already empty
     * */
    // do nothing
  } else {
    num = op->get_child_num();
    for (int32_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (OB_FAIL(build_cte_op_pair(op->get_child(i), cte_pumps))) {
        LOG_WARN("failed to build cte pair");
      }
    }
    if (OB_SUCC(ret) && PHY_RECURSIVE_UNION_ALL == op->get_type()) {
      ObRecursiveUnionAll* r_union = static_cast<ObRecursiveUnionAll*>(op);
      if (0 == cte_pumps.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the fake cte table can not be null", K(ret));
      } else if (OB_FAIL(cte_pumps.pop_back(last_cte_table))) {
        LOG_WARN("pop back failed", K(ret));
      } else {
        ObFakeCTETable* cte_table = static_cast<ObFakeCTETable*>(last_cte_table);
        r_union->set_fake_cte_table(cte_table);
      }
    }
    if (OB_SUCC(ret) && PHY_FAKE_CTE_TABLE == op->get_type()) {
      if (OB_FAIL(cte_pumps.push_back(op))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}

DEFINE_TO_YSON_KV(ObTask, OB_ID(task_id), ob_task_id_, OB_ID(runner_svr), runner_svr_, OB_ID(ctrl_svr), ctrl_svr_,
    OB_ID(pkeys), partition_keys_);

int ObTask::serialize_tree(char* buf, int64_t buf_len, int64_t& pos, const ObPhyOperator& root) const
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;
  ObPhyOpSeriCtx seri_ctx;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec_ctx_ is null", K_(exec_ctx), K_(ser_phy_plan));
  } else if (ser_phy_plan_->is_dist_insert_or_replace_plan() && location_idx_ != OB_INVALID_INDEX) {
    // The values sent to different partitions in the distributed insert are different,
    // Use row_id_list to indicate the data that needs to be inserted in each partition, serialized
    // Only the rows indicated by row_id_list are serialized when ObExprValues
    seri_ctx.row_id_list_ = exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_);
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(OB_I(t1) serialization::encode_vi32(buf, buf_len, pos, root.get_type()))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(OB_I(t2) root.serialize(buf, buf_len, pos, seri_ctx))) {
    LOG_WARN("fail to serialize root", K(ret), "type", root.get_type(), "root", to_cstring(root));
  } else {
    // nop
    LOG_DEBUG("serialize phy operator",
        K(root.get_type()),
        "serialize_size",
        root.get_serialize_size(),
        "real_size",
        pos - old_pos,
        "left",
        buf_len - pos,
        "pos",
        pos,
        K(ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(root.get_type()))));
    if (PHY_UPDATE == root.get_type()) {
      LOG_DEBUG("serialized update op", K(root));
    } else if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
      const ObPhyOperator* index_scan_tree = static_cast<const ObTableScanWithIndexBack&>(root).get_index_scan_tree();
      if (OB_ISNULL(index_scan_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index scan tree is null");
      } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *index_scan_tree))) {
        LOG_WARN("serialize index scan tree failed", K(ret));
      }
    } else if (PHY_TABLE_LOOKUP == root.get_type()) {
      ObPhyOperator* multi_partition_scan_tree = static_cast<const ObTableLookup&>(root).get_remote_plan();
      if (OB_ISNULL(multi_partition_scan_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index scan tree is null", K(ret));
      } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *multi_partition_scan_tree))) {
        LOG_WARN("deserialize tree failed", K(ret), K(buf_len), K(pos));
      }
    }
  }

  // Terminate serialization when meet ObReceive, as this op indicates
  if (OB_SUCC(ret) && !IS_RECEIVE(root.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_num(); ++i) {
      ObPhyOperator* child_op = root.get_child(i);
      if (OB_I(t3) OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(root.get_type()));
      } else if (OB_FAIL(OB_I(t4) serialize_tree(buf, buf_len, pos, *child_op))) {
        LOG_WARN("fail to serialize tree", K(ret));
      }
    }
  }

  return ret;
}

int ObTask::deserialize_tree(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;

  if (OB_FAIL(OB_I(t1) serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator",
        K(phy_operator_type),
        "type_str",
        ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)),
        K(pos));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_operator_by_type(static_cast<ObPhyOperatorType>(phy_operator_type), root))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else {
      if (OB_FAIL(OB_I(t3) root->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
      } else {
        LOG_DEBUG("deserialize phy operator",
            K(root->get_type()),
            "serialize_size",
            root->get_serialize_size(),
            "data_len",
            data_len,
            "pos",
            pos,
            "takes",
            data_len - pos);
      }
      if (OB_SUCC(ret) && PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root->get_type()) {
        ObPhyOperator* index_scan_tree = 0;
        ObTableScanWithIndexBack* table_scan = static_cast<ObTableScanWithIndexBack*>(root);
        if (OB_FAIL(deserialize_tree(buf, data_len, pos, phy_plan, index_scan_tree))) {
          LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
        } else {
          table_scan->set_index_scan_tree(index_scan_tree);
        }
      }

      if (OB_SUCC(ret) && PHY_TABLE_LOOKUP == root->get_type()) {
        ObPhyOperator* multi_partition_scan_tree = NULL;
        ObTableLookup* table_scan = static_cast<ObTableLookup*>(root);
        if (OB_FAIL(deserialize_tree(buf, data_len, pos, phy_plan, multi_partition_scan_tree))) {
          LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
        } else {
          table_scan->set_remote_plan(multi_partition_scan_tree);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_I(t5)(root->get_type() <= PHY_INVALID || root->get_type() >= PHY_END)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid operator type", N_TYPE, root->get_type());
    }
  }

  // Terminate serialization when meet ObReceive, as this op indicates
  if (OB_SUCC(ret) && !IS_RECEIVE(root->get_type())) {
    if (OB_FAIL(root->create_child_array(root->get_child_num()))) {
      LOG_WARN("create child array failed", K(ret), K(root->get_child_num()));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < root->get_child_num(); i++) {
      ObPhyOperator* child = NULL;
      if (OB_FAIL(OB_I(t6) deserialize_tree(buf, data_len, pos, phy_plan, child))) {
        LOG_WARN("fail to deserialize tree", K(ret));
      } else if (OB_FAIL(OB_I(t7) root->set_child(i, *child))) {
        LOG_WARN("fail to set child", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObTask::get_tree_serialize_size(const ObPhyOperator& root) const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  ObPhyOpSeriCtx seri_ctx;

  if (exec_ctx_ != NULL && ser_phy_plan_ != NULL && ser_phy_plan_->is_dist_insert_or_replace_plan() &&
      location_idx_ != OB_INVALID_INDEX) {
    const ObIArray<int64_t>* row_id_list = exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_);
    if (row_id_list != NULL) {
      seri_ctx.row_id_list_ = row_id_list;
    }
  }
  len += serialization::encoded_length_vi32(root.get_type());
  len += root.get_serialize_size(seri_ctx);
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    const ObPhyOperator* index_scan_tree = static_cast<const ObTableScanWithIndexBack&>(root).get_index_scan_tree();
    if (index_scan_tree != NULL) {
      len += get_tree_serialize_size(*index_scan_tree);
    }
  }
  if (PHY_TABLE_LOOKUP == root.get_type()) {
    const ObPhyOperator* multi_partition_scan_tree = static_cast<const ObTableLookup&>(root).get_remote_plan();
    if (multi_partition_scan_tree != NULL) {
      len += get_tree_serialize_size(*multi_partition_scan_tree);
    }
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  if (!IS_RECEIVE(root.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_num(); ++i) {
      ObPhyOperator* child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        // No error can be thrown here, but in the serialize phase,
        // it will check again whether there is a null child. So it is safe
        LOG_ERROR("null child op", K(i), K(root.get_child_num()), K(root.get_type()));
      } else {
        len += get_tree_serialize_size(*child_op);
      }
    }
  }

  return len;
}

int ObTask::assign_ranges(const ObIArray<ObNewRange>& ranges)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("assign ranges to task", K(ranges));
  FOREACH_CNT_X(it, ranges, OB_SUCC(ret))
  {
    if (OB_FAIL(ranges_.push_back(*it))) {
      LOG_WARN("push back range failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObMiniTask)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObMiniTask, ObTask));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ser_phy_plan_->is_new_engine()) {
    bool has_extend_root_spec = (extend_root_spec_ != NULL);
    OB_UNIS_ENCODE(has_extend_root_spec);
    if (OB_SUCC(ret) && has_extend_root_spec) {
      if (OB_FAIL(ObPxTreeSerializer::serialize_tree(buf, buf_len, pos, *extend_root_spec_, false /**is full tree*/))) {
        LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
                     buf, buf_len, pos, *extend_root_spec_, exec_ctx_->get_kit_store(), false /*is full tree*/))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      }
    }
  } else {
    bool has_extend_root = (extend_root_ != NULL);
    OB_UNIS_ENCODE(has_extend_root);
    if (OB_SUCC(ret) && has_extend_root) {
      if (OB_FAIL(serialize_tree(buf, buf_len, pos, *extend_root_))) {
        LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
      }
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObMiniTask)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObMiniTask, ObTask));
  if (OB_FAIL(ret)) {
  } else if (des_phy_plan_->is_new_engine()) {
    bool has_extend_spec = false;
    OB_UNIS_DECODE(has_extend_spec);
    if (OB_SUCC(ret) && has_extend_spec) {
      if (OB_ISNULL(exec_ctx_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(buf, data_len, pos, *des_phy_plan_, extend_root_spec_))) {
        LOG_WARN("fail deserialize tree", K(ret));
      } else if (OB_FAIL(extend_root_spec_->create_op_input(*exec_ctx_))) {
        LOG_WARN("create operator from spec failed", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(buf, data_len, pos, exec_ctx_->get_kit_store()))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      }
    }
  } else {
    bool has_extend_root = false;
    OB_UNIS_DECODE(has_extend_root);
    if (OB_SUCC(ret) && has_extend_root) {
      if (OB_FAIL(deserialize_tree(buf, data_len, pos, *des_phy_plan_, extend_root_))) {
        LOG_WARN("fail deserialize tree", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMiniTask)
{
  int64_t len = 0;
  if (ser_phy_plan_->is_new_engine()) {
    bool has_extend_root_spec = (extend_root_spec_ != NULL);
    BASE_ADD_LEN((ObMiniTask, ObTask));
    OB_UNIS_ADD_LEN(has_extend_root_spec);
    if (has_extend_root_spec) {
      if (OB_ISNULL(extend_root_spec_) || OB_ISNULL(exec_ctx_)) {
        LOG_ERROR("invalid argument", KP(extend_root_spec_), KP(exec_ctx_));
      } else {
        len += ObPxTreeSerializer::get_tree_serialize_size(*extend_root_spec_, false /*is fulltree*/);
        len += ObPxTreeSerializer::get_serialize_op_input_size(
            *extend_root_spec_, exec_ctx_->get_kit_store(), false /*is fulltree*/);
      }
    }
  } else {
    bool has_extend_root = (extend_root_ != NULL);
    BASE_ADD_LEN((ObMiniTask, ObTask));
    OB_UNIS_ADD_LEN(has_extend_root);
    if (has_extend_root) {
      if (OB_ISNULL(extend_root_)) {
        LOG_ERROR("invalid argument", KP(extend_root_));
      } else {
        len += get_tree_serialize_size(*extend_root_);
      }
    }
  }
  return len;
}

ObPingSqlTask::ObPingSqlTask()
    : trans_id_(), sql_no_(OB_INVALID_ID), task_id_(), exec_svr_(), part_keys_(), cur_status_(0)
{}

ObPingSqlTask::~ObPingSqlTask()
{}

OB_SERIALIZE_MEMBER(ObPingSqlTask, trans_id_, sql_no_, task_id_, part_keys_, cur_status_);

ObPingSqlTaskResult::ObPingSqlTaskResult() : err_code_(OB_SUCCESS), ret_status_(0)
{}

ObPingSqlTaskResult::~ObPingSqlTaskResult()
{}

OB_SERIALIZE_MEMBER(ObPingSqlTaskResult, err_code_, ret_status_);

OB_DEF_SERIALIZE(ObRemoteTask)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  ParamStore* ps_params = nullptr;
  // for serialize ObObjParam' param_meta_
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_) || OB_ISNULL(session_info_) || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("remote task not init", K(ret), K_(remote_sql_info), K_(session_info), K(ps_params));
  } else {
    tenant_id = session_info_->get_effective_tenant_id();
    param_meta_count = ps_params->count();
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_schema_version_,
      sys_schema_version_,
      runner_svr_,
      ctrl_svr_,
      task_id_,
      remote_sql_info_->use_ps_,
      remote_sql_info_->remote_sql_,
      *ps_params,
      tenant_id,
      *session_info_,
      remote_sql_info_->is_batched_stmt_,
      dependency_tables_);
  OB_UNIS_ENCODE(param_meta_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
    OB_UNIS_ENCODE(ps_params->at(i).get_param_meta());
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRemoteTask)
{
  int64_t len = 0;
  ParamStore* ps_params = nullptr;
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_) || OB_ISNULL(session_info_) || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    LOG_WARN("remote task not init", K_(remote_sql_info), K_(session_info), K(ps_params));
  } else {
    int64_t tenant_id = session_info_->get_effective_tenant_id();
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        tenant_schema_version_,
        sys_schema_version_,
        runner_svr_,
        ctrl_svr_,
        task_id_,
        remote_sql_info_->use_ps_,
        remote_sql_info_->remote_sql_,
        *ps_params,
        tenant_id,
        *session_info_,
        remote_sql_info_->is_batched_stmt_,
        dependency_tables_);
    param_meta_count = ps_params->count();
    OB_UNIS_ADD_LEN(param_meta_count);
    for (int64_t i = 0; i < param_meta_count; ++i) {
      OB_UNIS_ADD_LEN(ps_params->at(i).get_param_meta());
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObRemoteTask)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  ParamStore* ps_params = nullptr;
  ObObjMeta tmp_meta;
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_) || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("remote task not init", K(ret), K_(remote_sql_info), K(ps_params));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_schema_version_,
      sys_schema_version_,
      runner_svr_,
      ctrl_svr_,
      task_id_,
      remote_sql_info_->use_ps_,
      remote_sql_info_->remote_sql_,
      *ps_params,
      tenant_id);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exec_ctx_->create_my_session(tenant_id))) {
      LOG_WARN("create my session failed", K(ret), K(tenant_id));
    } else {
      session_info_ = exec_ctx_->get_my_session();
      OB_UNIS_DECODE(*session_info_);
      OB_UNIS_DECODE(remote_sql_info_->is_batched_stmt_);
    }
    dependency_tables_.set_allocator(&(exec_ctx_->get_allocator()));
    OB_UNIS_DECODE(dependency_tables_);
    // DESERIALIZE param_meta_count if 0, (1) params->count() ==0 (2) old version -> new version
    // for (2) just set obj.meta as param_meta
    OB_UNIS_DECODE(param_meta_count);
    if (OB_SUCC(ret)) {
      if (param_meta_count > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
          OB_UNIS_DECODE(tmp_meta);
          ps_params->at(i).set_param_meta(tmp_meta);
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ps_params->count(); ++i) {
          ps_params->at(i).set_param_meta();
        }
      }
    }
  }
  return ret;
}

int ObRemoteTask::assign_dependency_tables(const DependenyTableStore& dependency_tables)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dependency_tables_.assign(dependency_tables))) {
    LOG_WARN("failed to assign file list", K(ret));
  }
  return ret;
}

DEFINE_TO_YSON_KV(ObRemoteTask, OB_ID(task_id), task_id_, OB_ID(runner_svr), runner_svr_, OB_ID(ctrl_svr), ctrl_svr_);

}  // namespace sql
}  // namespace oceanbase
