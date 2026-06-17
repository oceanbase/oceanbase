/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_random_mds_helper.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tablet/ob_tablet_random_replay_executor.h"
#define USING_LOG_PREFIX MDS

using oceanbase::sqlclient::ObISQLConnection;
using oceanbase::obrpc::ObBatchSetTabletAutoincSeqArg;

namespace oceanbase
{
namespace storage
{

int ObTabletRandomMdsArg::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletRandomMdsUserData> &random_info_datas,
    const ObBatchSetTabletAutoincSeqArg &autoinc_seq_arg)
{
  int ret = OB_SUCCESS;
  reset();
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(random_info_datas_.assign(random_info_datas))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(autoinc_seq_arg_.assign(autoinc_seq_arg))) {
    LOG_WARN("failed to assign", K(ret));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(ret), K(*this));
  }
  return ret;
}

void ObTabletRandomMdsArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  random_info_datas_.reset();
  tablet_ids_.reset();
  autoinc_seq_arg_.reset();
}
bool ObTabletRandomMdsArg::is_valid() const
{
  bool is_valid = (OB_INVALID_TENANT_ID != tenant_id_) && ls_id_.is_valid() && random_info_datas_.count() == tablet_ids_.count();
  for (int64_t i = 0; is_valid && i < random_info_datas_.count(); ++i) {
    is_valid &= random_info_datas_.at(i).is_valid() && tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

int ObTabletRandomMdsArg::assign(const ObTabletRandomMdsArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    if (OB_FAIL(random_info_datas_.assign(other.random_info_datas_))) {
      LOG_WARN("failed to assign random info datas", K(ret), K(other.random_info_datas_));
    } else if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
      LOG_WARN("failed to assign tablet ids", K(ret), K(other.tablet_ids_));
    } else if (OB_FAIL(autoinc_seq_arg_.assign(other.autoinc_seq_arg_))) {
      LOG_WARN("failed to assign", K(ret), K(other.autoinc_seq_arg_));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletRandomMdsArg, tenant_id_, ls_id_, random_info_datas_, tablet_ids_, autoinc_seq_arg_);

int ObTabletRandomMdsHelper::modify(
    const ObTabletRandomMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = arg.ls_id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool need_empty_shell_trigger = false;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(arg), KP(ls));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.random_info_datas_.count(); i++) {
    const ObTabletID &tablet_id = arg.tablet_ids_[i];
    const ObTabletRandomMdsUserData &data = arg.random_info_datas_[i]; // index range is checked by is_valid()
    if (OB_FAIL(set_tablet_random_mds(ls_id, tablet_id, scn, data, ctx))) {
      LOG_WARN("failed to modify", K(ret), K(ls_id), K(tablet_id), K(scn));
    }
  }
  if (OB_SUCC(ret) && arg.autoinc_seq_arg_.is_valid()) {
    if (OB_FAIL(ObTabletAutoincSeqRpcHandler::get_instance().batch_set_tablet_autoinc_seq_in_trans(*ls, arg.autoinc_seq_arg_, scn, ctx))) {
      LOG_WARN("failed to batch set tablet autoinc seq", K(ret), K(scn));
    }
  }
  LOG_INFO("modify tablet random data", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  return ret;
}

int ObTabletRandomMdsHelper::set_tablet_random_mds(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &replay_scn,
    const ObTabletRandomMdsUserData &data,
    mds::BufferCtx &ctx)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  if (!replay_scn.is_valid()) {
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret));
    } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::set(data, user_ctx, 0/*lock_timeout_us*/))) {
      LOG_WARN("failed to set mds data", K(ret));
    }
  } else {
    ObTabletRandomReplayExecutor replay_executor;
    if (CLICK_FAIL(replay_executor.init(ctx, replay_scn, data))) {
      LOG_WARN("failed to init replay executor", K(ret));
    } else if (CLICK_FAIL(replay_executor.execute(replay_scn, ls_id, tablet_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay mds", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletRandomMdsHelper::on_register(const char* buf, const int64_t len,  mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletRandomMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, SCN::invalid_scn(), ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}

int ObTabletRandomMdsHelper::on_replay(const char* buf, const int64_t len, share::SCN scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletRandomMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, scn, ctx))) {
    LOG_WARN("failed to register_process", K(ret));
  }
  return ret;
}


int ObTabletRandomMdsHelper::register_mds(const ObTabletRandomMdsArg &arg, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *isql_conn = nullptr;
  if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("incalid argument", K(ret), K(arg));
  } else {
    const int64_t size = arg.get_serialize_size();
    ObArenaAllocator allocator(ObMemAttr(common::OB_SERVER_TENANT_ID, "DldUpTabMeta"));
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", K(ret));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize arg", K(ret));
    } else if (OB_FAIL(static_cast<observer::ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, transaction::ObTxDataSourceType::TABLET_RANDOM, buf, pos))) {
      LOG_WARN("failed to register mds", K(ret));
    }
  }
  return ret;
}

int ObTabletRandomMdsHelper::set_auto_random_size_for_create(
    const uint64_t tenant_id,
    const obrpc::ObBatchCreateTabletArg &create_arg,
    const ObIArray<int64_t> &auto_random_size_arr,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!auto_random_size_arr.empty()) {
    const ObLSID &ls_id = create_arg.id_;
    ObTabletRandomMdsArg arg;
    ObArray<ObTabletID> random_tablet_ids;
    ObArray<ObTabletRandomMdsUserData> random_info_datas;
    ObTabletRandomMdsUserData tmp_data;
    const ObBatchSetTabletAutoincSeqArg invalid_autoinc_seq_arg;
    if (OB_UNLIKELY(!create_arg.table_schemas_.empty() || create_arg.create_tablet_schemas_.count() != auto_random_size_arr.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg for set auto random size", K(ret), K(create_arg.table_schemas_.count()), K(create_arg.create_tablet_schemas_.count()),
          K(auto_random_size_arr.count()), K(create_arg));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < create_arg.tablets_.count(); i++) {
      const ObIArray<ObTabletID> &tablet_ids = create_arg.tablets_[i].tablet_ids_;
      const ObIArray<int64_t> &table_schema_idx_arr = create_arg.tablets_[i].table_schema_index_;
      if (OB_UNLIKELY(tablet_ids.count() != table_schema_idx_arr.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table schema idx arr", K(ret), K(tablet_ids.count()), K(table_schema_idx_arr.count()));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); j++) {
        const ObTabletID &tablet_id = tablet_ids.at(j);
        const int64_t schema_idx = table_schema_idx_arr.at(j);
        if (OB_UNLIKELY(schema_idx >= auto_random_size_arr.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("schema idx out of range", K(ret), K(i), K(j), K(create_arg), K(auto_random_size_arr));
        } else {
          const int64_t auto_random_size = auto_random_size_arr.at(schema_idx);
          if (OB_INVALID_SIZE != auto_random_size) {
            if (OB_FAIL(tmp_data.init_random_size(auto_random_size))) {
              LOG_WARN("failed to set random data", K(ret));
            } else if (OB_FAIL(random_tablet_ids.push_back(tablet_id))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (OB_FAIL(random_info_datas.push_back(tmp_data))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (tablet_ids.count() >= ObTabletRandomMdsArg::BATCH_TABLET_CNT) {
              if (OB_FAIL(arg.init(tenant_id, ls_id, random_tablet_ids, random_info_datas, invalid_autoinc_seq_arg))) {
                LOG_WARN("failed to init tablet random mds arg", K(ret));
              } else if (OB_FAIL(register_mds(arg, trans))) {
                LOG_WARN("failed to register mds", K(ret));
              } else {
                random_tablet_ids.reuse();
                random_info_datas.reuse();
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !random_tablet_ids.empty()) {
      if (OB_FAIL(arg.init(tenant_id, ls_id, random_tablet_ids, random_info_datas, invalid_autoinc_seq_arg))) {
        LOG_WARN("failed to init tablet random mds arg", K(ret));
      } else if (OB_FAIL(register_mds(arg, trans))) {
        LOG_WARN("failed to register mds", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletRandomMdsHelper::set_autoinc_seq_for_create(
    const ObTableSchema &table_schema,
    const int64_t prev_high_bound_val,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const ObPartitionLevel part_level = table_schema.get_part_level();
  ObPartition **part_array = table_schema.get_part_array();
  const int64_t part_num = table_schema.get_partition_num();
  ObSEArray<ObLSID, 3> ls_ids;
  ObSEArray<ObTabletID, 3> tablet_ids;
  if (OB_UNLIKELY(part_level != PARTITION_LEVEL_ONE || OB_ISNULL(part_array) || part_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part", K(ret), K(table_id), K(part_level), KP(part_array), K(part_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      if (OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i), K(table_id));
      } else if (OB_FAIL(tablet_ids.push_back(part_array[i]->get_tablet_id()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
      LOG_WARN("failed to get tablet ls", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t prev_max_seq = prev_high_bound_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
      if (OB_ISNULL(part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(i), K(table_id));
      } else {
        const ObLSID &ls_id = ls_ids.at(i);
        const ObRowkey &high_bound_val = part_array[i]->get_high_bound_val();
        const ObTabletID &tablet_id = part_array[i]->get_tablet_id();
        int64_t max_seq = 0;
        ObTabletRandomMdsArg arg;
        if (OB_UNLIKELY(1 != high_bound_val.get_obj_cnt() || !tablet_id.is_valid() || i >= ls_ids.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected partition", K(ret), K(table_id), K(i), K(tablet_id), K(ls_ids.count()), K(table_schema.get_table_id()), KPC(part_array[i]));
        } else if (OB_FAIL(high_bound_val.get_obj_ptr()[0].get_int(max_seq))) {
          LOG_WARN("fail to get int from high bound val", K(ret), K(high_bound_val));
        } else if (OB_UNLIKELY(prev_max_seq > max_seq - 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("random part high bound is not increasing", K(ret), K(prev_high_bound_val), K(prev_max_seq), K(max_seq), K(table_schema));
        } else {
          ObBatchSetTabletAutoincSeqArg autoinc_arg;
          ObMigrateTabletAutoincSeqParam autoinc_param;
          autoinc_arg.tenant_id_ = tenant_id;
          autoinc_arg.is_tablet_creating_ = true;
          autoinc_arg.ls_id_ = ls_id;
          autoinc_param.src_tablet_id_ = tablet_id;
          autoinc_param.dest_tablet_id_ = tablet_id;
          autoinc_param.autoinc_seq_ = prev_max_seq;
          autoinc_param.autoinc_seq_end_ = max_seq - 1;
          const ObArray<ObTabletID> empty_tablet_ids;
          const ObArray<ObTabletRandomMdsUserData> empty_datas;
          if (OB_FAIL(autoinc_arg.autoinc_params_.push_back(autoinc_param))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(arg.init(tenant_id, ls_id, empty_tablet_ids, empty_datas, autoinc_arg))) {
            LOG_WARN("failed to init tablet random mds arg", K(ret));
          } else if (OB_FAIL(register_mds(arg, trans))) {
            LOG_WARN("failed to register mds", K(ret));
          } else {
            prev_max_seq = max_seq;
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletRandomMdsHelper::get_valid_timeout(const int64_t abs_timeout_us, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  if (abs_timeout_us == 0) { // e.g., select for update nowait
    timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S;
  } else if (OB_UNLIKELY(abs_timeout_us <= cur_time)) {
    ret = OB_TIMEOUT;
    LOG_WARN("timed out", K(ret), K(abs_timeout_us), K(cur_time));
  } else {
    timeout_us = abs_timeout_us - cur_time;
  }
  return ret;
}

int ObTabletRandomMdsHelper::get_random_data_with_timeout(const ObTablet &tablet, ObTabletRandomMdsUserData &random_data, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
    LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
  } else if (OB_FAIL(tablet.ObITabletMdsInterface::get_random_part_data(random_data, timeout_us))) {
    LOG_WARN("failed to get random data", K(ret), K(abs_timeout_us), K(timeout_us), K(tablet.get_tablet_meta()));
  }
  return ret;
}

int ObModifyAutoRandomSizeOp::operator()(ObTabletRandomMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (data.is_active() && OB_FAIL(data.set_random_size(auto_random_size_))) {
    LOG_WARN("failed to set random size", K(ret));
  }
  return ret;
}


int ObModifyInactiveOp::operator()(ObTabletRandomMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data.set_is_active(false))) {
    LOG_WARN("failed to set inactive", K(ret));
  }
  return ret;
}

int ObTabletRandomMdsHelper::modify_auto_random_size(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t auto_random_size,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObModifyAutoRandomSizeOp op(auto_random_size);
  if (OB_FAIL(modify_tablet_random_mds_(tenant_id, tablet_ids, abs_timeout_us, op, trans))) {
    LOG_WARN("failed to modify tablet random mds", K(ret));
  }
  return ret;
}

int ObTabletRandomMdsHelper::modify_inactive(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObModifyInactiveOp op;
  if (OB_FAIL(modify_tablet_random_mds_(tenant_id, tablet_ids, abs_timeout_us, op, trans))) {
    LOG_WARN("failed to modify tablet random mds", K(ret));
  }
  return ret;
}

template<typename F>
int ObTabletRandomMdsHelper::modify_tablet_random_mds_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    F &&op,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!tablet_ids.empty()) {
    ObTabletRandomMdsArg arg;
    ObArray<std::pair<ObLSID, ObTabletID>> ls_tablets;
    ObArray<ObTabletID> this_batch_tablet_ids;
    if (OB_FAIL(ObTabletBindingMdsHelper::get_sorted_ls_tablets(tenant_id, tablet_ids, ls_tablets, trans))) {
      LOG_WARN("failed to get sorted ls tablets", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); i++) {
      const ObLSID &ls_id = ls_tablets.at(i).first;
      const ObTabletID &tablet_id = ls_tablets.at(i).second;
      const bool is_last_or_next_ls_id_changed = i == ls_tablets.count() - 1 || ls_id != ls_tablets.at(i+1).first;
      if (OB_FAIL(this_batch_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (is_last_or_next_ls_id_changed || this_batch_tablet_ids.count() >= ObTabletRandomMdsArg::BATCH_TABLET_CNT) {
        ObArray<ObTabletRandomMdsUserData> datas;
        ObBatchSetTabletAutoincSeqArg empty_autoinc_arg;
        if (OB_FAIL(get_tablet_random_mds_by_rpc(tenant_id, ls_id, this_batch_tablet_ids, abs_timeout_us, datas))) {
          LOG_WARN("failed to get tablet random mds", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < datas.count(); j++) {
          ret = op(datas.at(j));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(arg.init(tenant_id, ls_id, this_batch_tablet_ids, datas, empty_autoinc_arg))) {
          LOG_WARN("failed to init", K(ret));
        } else if (OB_FAIL(register_mds(arg, trans))) {
          LOG_WARN("failed to register mds", K(ret));
        }

        if (OB_SUCC(ret)) {
          this_batch_tablet_ids.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!this_batch_tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch not consumed out", K(ret), K(this_batch_tablet_ids));
    }
  }
  return ret;
}

int ObTabletRandomMdsHelper::get_tablet_random_mds_by_rpc(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletRandomMdsUserData> &datas)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  share::ObLocationService *location_service = nullptr;
  ObAddr leader_addr;
  obrpc::ObBatchGetTabletRandomArg arg;
  obrpc::ObBatchGetTabletRandomRes res;
  if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)
      || OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service or location_cache is null", K(ret), KP(srv_rpc_proxy), KP(location_service));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id, tablet_ids, true/*check_committed*/))) {
    LOG_WARN("failed to init arg", K(ret), K(tenant_id), K(ls_id), K(ls_id));
  } else {
    bool force_renew = false;
    bool finish = false;
    for (int64_t retry_times = 0; OB_SUCC(ret) && !finish; retry_times++) {
      int64_t timeout_us = 0;
      if (OB_FAIL(location_service->get_leader(cluster_id, tenant_id, ls_id, force_renew, leader_addr))) {
        LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(get_valid_timeout(abs_timeout_us, timeout_us))) {
        LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
      } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr).timeout(timeout_us).batch_get_tablet_random(arg, res))) {
        LOG_WARN("fail to batch get tablet random", K(ret), K(retry_times), K(abs_timeout_us));
      } else {
        finish = true;
      }
      if (OB_FAIL(ret)) {
        force_renew = true;
        if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_GET_LOCATION_TIME_OUT == ret || OB_NOT_MASTER == ret
            || OB_ERR_SHARED_LOCK_CONFLICT == ret || OB_LS_OFFLINE == ret
            || OB_NOT_INIT == ret || OB_LS_NOT_EXIST == ret || OB_TABLET_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret || OB_LS_LOCATION_NOT_EXIST == ret) {
          // overwrite ret
          if (OB_UNLIKELY(ObTimeUtility::current_time() > abs_timeout_us)) {
            ret = OB_TIMEOUT;
            LOG_WARN("timeout", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(THIS_WORKER.check_status())) {
            LOG_WARN("failed to check status", K(ret), K(abs_timeout_us));
          } else if (retry_times >= 3) {
            ob_usleep<common::ObWaitEventIds::STORAGE_AUTOINC_FETCH_RETRY_SLEEP>(100 * 1000L); // 100ms
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(datas.assign(res.random_datas_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObTabletRandomMdsHelper::batch_get_tablet_random(
    const int64_t abs_timeout_us,
    const obrpc::ObBatchGetTabletRandomArg &arg,
    obrpc::ObBatchGetTabletRandomRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSService *ls_service = MTL(ObLSService *);
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObRole role = INVALID_ROLE;
      if (OB_ISNULL(ls_service) || OB_ISNULL(log_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ls_service or log_service", K(ret));
      } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
        LOG_WARN("get ls failed", K(ret), K(arg));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls", K(ret), K(arg.ls_id_));
      } else if (OB_FAIL(ls->get_ls_role(role))) {
        LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else if (OB_UNLIKELY(ObRole::LEADER != role)) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls not leader", K(ret), K(MTL_ID()), K(arg.ls_id_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); i++) {
          const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
          ObTabletHandle tablet_handle;
          ObTabletRandomMdsUserData data;

          if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(abs_timeout_us));
          } else if (OB_FAIL(get_random_data_with_timeout(*tablet_handle.get_obj(), data, abs_timeout_us))) {
            LOG_WARN("failed to get random data", K(ret), K(abs_timeout_us));
          } else if (OB_FAIL(res.random_datas_.push_back(data))) {
            LOG_WARN("failed to push back", K(ret));
          }

          // currently not support to read uncommitted mds set by this transaction, so check and avoid such usage
          if (OB_SUCC(ret) && arg.check_committed_) {
            ObTabletRandomMdsUserData tmp_data;
            mds::MdsWriter writer;// will be removed later
            mds::TwoPhaseCommitState trans_stat;// will be removed later
            share::SCN trans_version;// will be removed later
            if (OB_FAIL(tablet_handle.get_obj()->get_latest_random_part_data(tmp_data, writer, trans_stat, trans_version))) {
              if (OB_EMPTY_RESULT == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get latest random data", K(ret));
              }
            } else if (OB_UNLIKELY(mds::TwoPhaseCommitState::ON_COMMIT != trans_stat)) {
              ret = OB_EAGAIN;
              LOG_WARN("check committed failed", K(ret), K(tenant_id), K(arg.ls_id_), K(tablet_id), K(tmp_data));
            }
          }
        }
      }
    }
  }
  return ret;
}

}
}
