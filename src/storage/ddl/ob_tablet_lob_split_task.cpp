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
#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_lob_split_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{

using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;
using namespace omt;
using namespace name;
using namespace transaction;
using namespace blocksstable;
using namespace compaction;

namespace storage
{

DEFINE_SERIALIZE(ObLobIdItem)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, id_.lob_id_))) {
    LOG_WARN("Fail to encode key", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, id_.tablet_id_))) {
    LOG_WARN("Fail to encode key", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, tablet_idx_))) {
    LOG_WARN("Fail to encode key", K(ret));
  }

  return ret;
}

DEFINE_DESERIALIZE(ObLobIdItem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&id_.lob_id_)))) {
    LOG_WARN("fail to decode key_", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t*>(&id_.tablet_id_)))) {
    LOG_WARN("fail to decode key_", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &tablet_idx_))) {
    LOG_WARN("fail to decode key_", K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLobIdItem)
{
  int64_t size = 0;

  size += serialization::encoded_length_vi64(id_.lob_id_);
  size += serialization::encoded_length_vi64(id_.tablet_id_);
  size += serialization::encoded_length_vi64(tablet_idx_);

  return size;
}

ObLobSplitParam::~ObLobSplitParam()
{
  parallel_datum_rowkey_list_.reset();
  rowkey_allocator_.reset();
}

int ObLobSplitParam::assign(const ObLobSplitParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    ori_lob_meta_tablet_id_ = other.ori_lob_meta_tablet_id_;
    schema_version_ = other.schema_version_;
    data_format_version_ = other.data_format_version_;
    parallelism_ = other.parallelism_;
    compaction_scn_ = other.compaction_scn_;
    compat_mode_ = other.compat_mode_;
    task_id_ = other.task_id_;
    source_table_id_ = other.source_table_id_;
    dest_schema_id_ = other.dest_schema_id_;
    consumer_group_id_ = other.consumer_group_id_;
    split_sstable_type_ = other.split_sstable_type_;
    min_split_start_scn_ = other.min_split_start_scn_;
    if (OB_FAIL(new_lob_tablet_ids_.assign(other.new_lob_tablet_ids_))) {
      LOG_WARN("failed to assign new_lob_tablet_ids_", K(ret));
    } else if (OB_FAIL(lob_col_idxs_.assign(other.lob_col_idxs_))) {
      LOG_WARN("failed to assign lob_col_idxs_", K(ret));
    } else if (OB_FAIL(parallel_datum_rowkey_list_.prepare_allocate(other.parallel_datum_rowkey_list_.count()))) {
      LOG_WARN("prepare alloc failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.parallel_datum_rowkey_list_.count(); i++) {
        if (OB_FAIL(other.parallel_datum_rowkey_list_.at(i).deep_copy(parallel_datum_rowkey_list_.at(i), rowkey_allocator_))) {
          // deep copy needed.
          LOG_WARN("alloc range buf failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLobSplitParam::init(const ObLobSplitParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("fail to assign from input", K(other));
  }
  return ret;
}

int ObLobSplitParam::init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(arg));
  } else {
    tenant_id_              = arg.tenant_id_;
    ls_id_                  = arg.ls_id_;
    ori_lob_meta_tablet_id_ = arg.source_tablet_id_;
    schema_version_         = arg.schema_version_;
    data_format_version_    = arg.data_format_version_;
    task_id_                = arg.task_id_;
    source_table_id_        = arg.source_table_id_;
    dest_schema_id_         = arg.dest_schema_id_;
    consumer_group_id_      = arg.consumer_group_id_;
    split_sstable_type_     = arg.split_sstable_type_;
    parallelism_            = arg.parallel_datum_rowkey_list_.count() - 1;
    compaction_scn_         = arg.compaction_scn_;
    min_split_start_scn_    = arg.min_split_start_scn_;
    if (OB_FAIL(parallel_datum_rowkey_list_.assign(arg.parallel_datum_rowkey_list_))) { // shallow cpy.
      LOG_WARN("assign failed", K(ret), "parall_info", arg.parallel_datum_rowkey_list_);
    } else if (OB_FAIL(ObTabletSplitUtil::get_split_dest_tablets_info(ls_id_, ori_lob_meta_tablet_id_, new_lob_tablet_ids_, compat_mode_))) {
      LOG_WARN("get split dest tablets failed", K(ret), K(arg));
    } else if (OB_FAIL(lob_col_idxs_.assign(arg.lob_col_idxs_))) {
      LOG_WARN("failed to assign lob_col_idxs_", K(ret));
    }
  }
  return ret;
}

int ObLobSplitParam::init(const obrpc::ObTabletSplitArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(arg));
  } else {
    tenant_id_              = MTL_ID();
    ls_id_                  = arg.ls_id_;
    ori_lob_meta_tablet_id_ = arg.source_tablet_id_;
    schema_version_         = arg.schema_version_;
    data_format_version_    = arg.data_format_version_;
    task_id_                = arg.task_id_;
    source_table_id_        = arg.table_id_;
    dest_schema_id_         = arg.lob_table_id_;
    consumer_group_id_      = arg.consumer_group_id_;
    split_sstable_type_     = arg.split_sstable_type_;
    parallelism_            = arg.parallel_datum_rowkey_list_.count() - 1;
    compaction_scn_         = arg.compaction_scn_;
    min_split_start_scn_    = arg.min_split_start_scn_;
    ObArray<ObTabletID> unused_tablet_ids;
    if (OB_FAIL(ObTabletSplitUtil::get_split_dest_tablets_info(ls_id_, ori_lob_meta_tablet_id_, unused_tablet_ids, compat_mode_))) {
      LOG_WARN("get split dest tablets failed", K(ret), K(arg));
    } else if (OB_FAIL(parallel_datum_rowkey_list_.assign(arg.parallel_datum_rowkey_list_))) { // shallow cpy.
      LOG_WARN("assign failed", K(ret), "parall_info", arg.parallel_datum_rowkey_list_);
    } else if (OB_FAIL(new_lob_tablet_ids_.assign(arg.dest_tablets_id_))) {
      LOG_WARN("failed to assign dest_tablets_id_", K(ret));
    } else if (OB_FAIL(lob_col_idxs_.assign(arg.lob_col_idxs_))) {
      LOG_WARN("failed to assign lob_col_idxs_", K(ret));
    }
  }
  return ret;
}

int ObLobSplitContext::init(const ObLobSplitParam& param)
{
  int ret = OB_SUCCESS;
  sub_maps_.set_block_allocator(m_allocator_);
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(init_maps(param))) {
    LOG_WARN("fail to init sort maps", K(ret), K(param));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_,
                                               param.ori_lob_meta_tablet_id_,
                                               lob_meta_tablet_handle_,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::check_satisfy_split_condition(
      ls_handle_, lob_meta_tablet_handle_, param.new_lob_tablet_ids_, param.compaction_scn_, param.min_split_start_scn_))) {
    if (OB_NEED_RETRY == ret) {
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_WARN("wait to satisfy the data split condition", K(ret), K(param));
      }
    } else {
      LOG_WARN("check satisfy split condition failed", K(ret), K(param));
    }
  } else if (OB_FAIL(get_dst_lob_tablet_ids(param))) {
    LOG_WARN("fail to get dst lob tablet ids", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::convert_rowkey_to_range(range_allocator_, param.parallel_datum_rowkey_list_, main_table_ranges_))) {
    LOG_WARN("convert to range failed", K(ret), "parall_info", param.parallel_datum_rowkey_list_);
  } else {
    is_inited_ = true;
    LOG_INFO("show main tablet split info", K(param));
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObLobSplitContext::get_dst_lob_tablet_ids(const ObLobSplitParam& param)
{
  int ret = OB_SUCCESS;
  // get new lob tablet ids
  ObTabletBindingMdsUserData ddl_data;
  ObTabletSplitMdsUserData lob_meta_split_data;
  ObTabletCreateDeleteMdsUserData user_data;
  if (OB_ISNULL(lob_meta_tablet_handle_.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(param));
  } else if (OB_FAIL(lob_meta_tablet_handle_.get_obj()->get_all_tables(lob_meta_table_store_iterator_))) {
    LOG_WARN("failed to get all sstables", K(ret), K(param));
  } else if (FALSE_IT(main_tablet_id_ = lob_meta_tablet_handle_.get_obj()->get_tablet_meta().data_tablet_id_)) {
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_,
                                               main_tablet_id_,
                                               main_tablet_handle_,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(param));
  } else if (OB_ISNULL(main_tablet_handle_.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(param));
  } else if (OB_FAIL(main_tablet_handle_.get_obj()->get_all_tables(main_table_store_iterator_))) {
    LOG_WARN("failed to get all sstables", K(ret), K(param));
  } else if (OB_FAIL(main_tablet_handle_.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("fail to get ddl data", K(ret), K(main_tablet_handle_));
  } else if (FALSE_IT(is_lob_piece_ = (param.ori_lob_meta_tablet_id_ == ddl_data.lob_piece_tablet_id_))) {
  } else if (OB_FAIL(lob_meta_tablet_handle_.get_obj()->get_split_data(lob_meta_split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(lob_meta_split_data.get_split_dst_tablet_ids(new_lob_tablet_ids_))) {
    LOG_WARN("failed to get split dst tablet ids", K(ret));
  } else if (OB_FAIL(lob_meta_tablet_handle_.get_obj()->ObITabletMdsInterface::get_tablet_status(
          share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", K(ret));
  } else if (user_data.start_split_commit_version_ <= 0) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to fetch the newest mds", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::check_sstables_skip_data_split(ls_handle_, lob_meta_table_store_iterator_,
      new_lob_tablet_ids_, user_data.start_split_commit_version_/*lob_major_snapshot*/, skipped_split_major_keys_))) {
    LOG_WARN("check sstables skip data split failed", K(ret));
  } else {
    ObTabletHandle dst_lob_tablet_handle;
    for (int64_t i = 0; i < new_lob_tablet_ids_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_,
                                            new_lob_tablet_ids_.at(i),
                                            dst_lob_tablet_handle,
                                            ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("get tablet handle failed", K(ret), K(new_lob_tablet_ids_), K(i));
      } else if (OB_ISNULL(dst_lob_tablet_handle.get_obj())) {
        ret = OB_ERR_SYS;
        LOG_WARN("tablet handle is null", K(ret), K(new_lob_tablet_ids_.at(i)));
      } else if (OB_FAIL(new_main_tablet_ids_.push_back(dst_lob_tablet_handle.get_obj()->get_tablet_meta().data_tablet_id_))) {
        LOG_WARN("fail to push new lob meta tablet ids");
      }
    }
  }
  LOG_INFO("init for get related tablet ids", K(ret), K(main_tablet_id_), K(new_main_tablet_ids_),
           K(param.ori_lob_meta_tablet_id_), K(new_lob_tablet_ids_));
  return ret;
}

int ObLobSplitContext::init_maps(const ObLobSplitParam& param)
{
  int ret = OB_SUCCESS;
  ObLobIdMap* submap = nullptr;
  const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
  const int64_t expire_timestamp = 0;
  const int64_t total_sort_memory_limit = EACH_SORT_MEMORY_LIMIT * param.parallelism_;
  const uint64_t tenant_id = param.tenant_id_;
  if (OB_ISNULL(total_map_ = reinterpret_cast<ObLobIdMap*>(allocator_.alloc(sizeof(ObLobIdMap))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc sub map", K(ret));
  } else if (FALSE_IT(total_map_ = new(total_map_)ObLobIdMap(allocator_))) {
  } else if (OB_FAIL(total_map_->init(total_sort_memory_limit, file_buf_size, expire_timestamp, tenant_id, &comparer_))) {
    LOG_WARN("fail to init external sorter", K(ret));
  }
  for (int64_t i = 0; i < param.parallelism_ && OB_SUCC(ret); i++) {
    if (OB_ISNULL(submap = reinterpret_cast<ObLobIdMap*>(allocator_.alloc(sizeof(ObLobIdMap))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc sub map", K(ret));
    } else if (FALSE_IT(submap = new(submap)ObLobIdMap(allocator_))) {
    } else if (OB_FAIL(submap->init(EACH_SORT_MEMORY_LIMIT, file_buf_size, expire_timestamp, tenant_id, &comparer_))) {
      LOG_WARN("fail to init external sorter", K(ret));
    } else if (OB_FAIL(sub_maps_.push_back(submap))) {
      LOG_WARN("fail to push back sub map", K(ret));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(submap)) {
      submap->clean_up();
      allocator_.free(submap);
    }
  }
  return ret;
}

void ObLobSplitContext::destroy()
{
  ls_handle_.reset();
  if (total_map_ != nullptr) {
    total_map_->clean_up();
    allocator_.free(total_map_);
    total_map_ = nullptr;
  }
  for (int64_t i = 0; i < sub_maps_.count(); i++) {
    if (sub_maps_.at(i) != nullptr) {
      sub_maps_.at(i)->clean_up();
      allocator_.free(sub_maps_.at(i));
    }
  }
  sub_maps_.reset();
  new_main_tablet_ids_.reset();
  new_lob_tablet_ids_.reset();
  main_table_store_iterator_.reset();
  lob_meta_table_store_iterator_.reset();
  m_allocator_.reset();
  allocator_.reset();
  main_table_ranges_.reset();
  skipped_split_major_keys_.reset();
  range_allocator_.reset();
}

bool ObLobIdItemCompare::operator()(const ObLobIdItem *left, const ObLobIdItem *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(common::OB_SUCCESS != result_code_)) {
    //do nothing
  } else if (OB_UNLIKELY(NULL == left)
             || OB_UNLIKELY(NULL == right)) {
    result_code_ = common::OB_INVALID_ARGUMENT;
    LOG_WARN_RET(result_code_, "Invaid argument, ", KP(left), KP(right), K_(result_code));
  } else {
    int cmp_ret = left->cmp(*right);
    bool_ret = (cmp_ret < 0);
  }
  return bool_ret;
}


ObTabletLobBuildMapTask::ObTabletLobBuildMapTask()
  : ObITask(TASK_TYPE_LOB_BUILD_MAP), is_inited_(false), task_id_(0),
    rk_cnt_(0), param_(nullptr), ctx_(nullptr),
    allocator_("LobBuildMap", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID())
{
}

ObTabletLobSplitDag::ObTabletLobSplitDag()
  : ObIDag(ObDagType::DAG_TYPE_LOB_SPLIT), is_inited_(false), param_(), context_()
{
}

ObTabletLobSplitDag::~ObTabletLobSplitDag()
{
  context_.destroy();
}

int ObTabletLobSplitDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLobSplitParam *tmp_param = static_cast<const ObLobSplitParam *>(param);
  if (OB_UNLIKELY(nullptr == tmp_param || !tmp_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(tmp_param));
  } else if (OB_FAIL(param_.init(*tmp_param))) {
    LOG_WARN("init tablet split param failed", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(param_));
  } else if (OB_FAIL(context_.init(param_))) {
    LOG_WARN("init failed", K(ret));
  } else {
    consumer_group_id_ = tmp_param->consumer_group_id_;
    is_inited_ = true;
  }
  return ret;
}

bool ObTabletLobSplitDag::operator==(const ObIDag &other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObTabletLobSplitDag &dag = static_cast<const ObTabletLobSplitDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid())) {
      tmp_ret = OB_ERR_SYS;
      LOG_ERROR("invalid argument", K(tmp_ret), K(param_), K(dag.param_));
    } else {
      is_equal = (param_.tenant_id_ == dag.param_.tenant_id_) && (param_.ls_id_ == dag.param_.ls_id_) &&
                 (param_.ori_lob_meta_tablet_id_ == dag.param_.ori_lob_meta_tablet_id_);
    }
  }
  return is_equal;
}

int ObTabletLobSplitDag::calc_total_row_count() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has not been inited ", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param_));
  } else if (context_.physical_row_count_ != 0) {
    // already calc, do nothing.
    LOG_TRACE("already calculated", K(context_.physical_row_count_));
  } else if (OB_FAIL(ObDDLUtil::get_tablet_physical_row_cnt(
                                  param_.ls_id_,
                                  param_.ori_lob_meta_tablet_id_,
                                  true, // calc_sstable = true;
                                  false, // calc_memtable = false;  because memtable has been frozen.
                                  context_.physical_row_count_))) {
      LOG_WARN("failed to get physical row count of tablet", K(ret), K(param_), K(context_));
  }
  LOG_INFO("calc row count of the src tablet", K(ret), K(context_));
  return ret;
}
int64_t ObTabletLobSplitDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    tmp_ret = OB_ERR_SYS;
    LOG_ERROR("table schema must not be NULL", K(tmp_ret), K(is_inited_), K(param_));
  } else {
    hash_val = param_.tenant_id_ + param_.ls_id_.hash()
             + param_.ori_lob_meta_tablet_id_.hash() + ObDagType::DAG_TYPE_LOB_SPLIT;
  }
  return hash_val;
}

int ObTabletLobSplitDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobSplitDag has not been initialized", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
      static_cast<int64_t>(param_.ls_id_.id()), static_cast<int64_t>(param_.ori_lob_meta_tablet_id_.id())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTabletLobSplitDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobSplitDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(param_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "Regen macro block split: src_tablet_id=%ld, parallelism=%ld, tenant_id=%lu, ls_id=%ld, schema_version=%ld",
      param_.ori_lob_meta_tablet_id_.id(), param_.parallelism_,
      param_.tenant_id_, param_.ls_id_.id(), param_.schema_version_))) {
    LOG_WARN("fail to fill comment", K(ret), K(param_));
  }
  return ret;
}

int ObTabletLobSplitDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletLobBuildMapTask *build_map_task = nullptr;
  ObTabletLobMergeMapTask *merge_map_task = nullptr;
  ObTabletLobWriteDataTask *write_data_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_task(build_map_task))) {
    LOG_WARN("allocate task failed", K(ret));
  } else if (OB_ISNULL(build_map_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(build_map_task->init(0, param_, context_))) {
    LOG_WARN("init build map task failed", K(ret));
  } else if (OB_FAIL(add_task(*build_map_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_task(merge_map_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_ISNULL(merge_map_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(merge_map_task->init(param_, context_))) {
    LOG_WARN("init merge map task failed", K(ret));
  } else if (OB_FAIL(build_map_task->add_child(*merge_map_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*merge_map_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_task(write_data_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_ISNULL(write_data_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(write_data_task->init(0, param_, context_))) {
    LOG_WARN("init write data task failed", K(ret));
  } else if (OB_FAIL(build_map_task->add_child(*write_data_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*write_data_task))) {
    LOG_WARN("add task failed");
  }
  return ret;
}

bool ObTabletLobSplitDag::ignore_warning()
{
  return OB_EAGAIN == dag_ret_
    || OB_NEED_RETRY == dag_ret_
    || OB_TASK_EXPIRED == dag_ret_;
}

int ObTabletLobSplitDag::report_lob_split_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobSplitDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else {
    obrpc::ObDDLBuildSingleReplicaResponseArg arg;
    ObAddr rs_addr;
    arg.tenant_id_ = param_.tenant_id_;
    arg.dest_tenant_id_ = param_.tenant_id_;
    arg.ls_id_ = param_.ls_id_;
    arg.dest_ls_id_ = param_.ls_id_;
    arg.tablet_id_ = param_.ori_lob_meta_tablet_id_;
    arg.source_table_id_ = param_.source_table_id_;
    arg.dest_schema_id_ = context_.main_tablet_id_.id(); // fill ori main tablet id
    arg.snapshot_version_ = 1L; // to avoid invalid only.
    arg.schema_version_ = param_.schema_version_;
    arg.dest_schema_version_ = param_.schema_version_;
    arg.ret_code_ = context_.data_ret_;
    arg.task_id_ = param_.task_id_;
    arg.execution_id_ = 1L;
    arg.server_addr_ = GCTX.self_addr();
    arg.row_inserted_ = context_.row_inserted_;
    arg.physical_row_count_ = context_.physical_row_count_;
    FLOG_INFO("send tablet split response to RS", K(ret), K(context_), K(arg), K(param_));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).build_ddl_single_replica_response(arg))) {
      LOG_WARN("fail to send build split tablet data response", K(ret), K(arg));
    }
    SERVER_EVENT_ADD("ddl", "replica_split_resp",
        "result", context_.data_ret_,
        "tenant_id", param_.tenant_id_,
        "source_tablet_id", param_.ori_lob_meta_tablet_id_.id(),
        "svr_addr", GCTX.self_addr(),
        "physical_row_count", context_.physical_row_count_,
        "split_total_rows", context_.row_inserted_,
        *ObCurTraceId::get_trace_id());
  }
  FLOG_INFO("lob tablet split finished", K(ret), K(context_.data_ret_));
  return ret;
}

ObTabletLobBuildMapTask::~ObTabletLobBuildMapTask()
{
}

int ObTabletLobBuildMapTask::init(const int64_t task_id, ObLobSplitParam &param,
    ObLobSplitContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletLobBuildMapTask has already been inited", K(ret));
  } else if (task_id < 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(param), K(context));
  } else {
    task_id_ = task_id;
    param_ = &param;
    ctx_ = &context;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletLobBuildMapTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObTabletLobSplitDag *dag = nullptr;
  bool is_data_split_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobBuildMapTask has not been inited before", K(ret));
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (OB_ISNULL(dag = static_cast<ObTabletLobSplitDag *>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KP(tmp_dag), KP(dag));
  } else if (OB_FAIL(dag->calc_total_row_count())) { // only calc row count once time for a task
    LOG_WARN("failed to calc task row count", K(ret));
  } else if (OB_SUCCESS != (ctx_->data_ret_)) {
    LOG_WARN("build map task has already failed", "ret", ctx_->data_ret_);
  } else if (ctx_->is_lob_piece_) {
    // lob piece has no data, do nothing
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->new_lob_tablet_ids_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else {
    ObArray<ObRowScan*> iters;
    common::ObArenaAllocator tmp_arena("RowScanIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletSplitMdsUserData split_data;
    const ObStorageSchema *main_storage_schema = nullptr;
    if (OB_FAIL(ctx_->main_tablet_handle_.get_obj()->get_split_data(split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
      LOG_WARN("fail to get split data", K(ret), K(ctx_->main_tablet_handle_));
    } else if (OB_FAIL(split_data.get_storage_schema(main_storage_schema))) {
      LOG_WARN("failed to get storage schema", K(ret));
    } else if (OB_FAIL(ObTabletLobSplitUtil::generate_col_param(main_storage_schema, rk_cnt_))) {
      LOG_WARN("fail to generate col param", K(ret), K(task_id_));
    } else if (OB_FAIL(ObTabletLobSplitUtil::open_rowscan_iters(param_->split_sstable_type_,
                                                         tmp_arena,
                                                         param_->source_table_id_,
                                                         ctx_->main_tablet_handle_,
                                                         ctx_->main_table_store_iterator_,
                                                         ctx_->main_table_ranges_.at(task_id_),
                                                         *main_storage_schema,
                                                         iters))) {
      LOG_WARN("fail to open iters", K(ret));
    } else if (OB_FAIL(build_sorted_map(iters))) {
      LOG_WARN("fail to do build sub map", K(ret));
    } else {
    #ifdef ERRSIM
      ret = OB_E(EventTable::EN_BLOCK_LOB_SPLIT_BEFORE_SSTABLES_SPLIT) OB_SUCCESS;
      if (OB_FAIL(ret)) { // errsim trigger.
        common::ObZone self_zone;
        ObString zone1_str("z1");
        if (OB_FAIL(SVR_TRACER.get_server_zone(GCTX.self_addr(), self_zone))) { // overwrite ret is expected.
          LOG_WARN("get server zone failed", K(ret));
        } else if (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, self_zone.str().ptr(), self_zone.str().length(),
            zone1_str.ptr(), zone1_str.length())) {
          ret = OB_EAGAIN;
          LOG_INFO("set eagain for tablet split", K(ret));
        }
      }
    #endif
      LOG_INFO("finish the lob build map task", K(ret), K(task_id_));
    }
    // close row scan iters
    for (int64_t i = 0; i < iters.count(); i++) {
      ObRowScan *iter = iters.at(i);
      if (OB_NOT_NULL(iter)) {
        iter->~ObRowScan();
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletLobBuildMapTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObTabletLobBuildMapTask *buildmap_task = nullptr;
  const int64_t next_task_id = task_id_ + 1;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobBuildMapTask has not been inited", K(ret));
  } else if (next_task_id >= param_->parallelism_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(tmp_dag->alloc_task(buildmap_task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(buildmap_task->init(next_task_id, *param_, *ctx_))) {
    LOG_WARN("fail to init lob build map task", K(ret));
  } else {
    next_task = buildmap_task;
    LOG_INFO("generate next lob build map task", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    if (OB_ITER_END != ret) {
      ctx_->data_ret_ = ret;
    }
  }
  return ret;
}

int ObTabletLobBuildMapTask::build_sorted_map(ObIArray<ObRowScan*>& iters)
{
  int ret = OB_SUCCESS;
  // get new lob tablet ids
  ObTabletHandle tablet_handle;
  ObTabletSplitMdsUserData src_split_data;
  ObSEArray<ObTabletSplitMdsUserData, 2> dst_split_datas;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobSplitTask is not inited", K(ret));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ctx_->ls_handle_,
                                               ctx_->main_tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(param));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(tablet_handle));
  } else if (ctx_->sub_maps_.count() <= task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub maps count is unexpected.", K(ret), K(ctx_->sub_maps_.count()), K(task_id_));
  } else if (OB_ISNULL(ctx_->sub_maps_.at(task_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub maps is null.", K(ret), K(ctx_->sub_maps_), K(task_id_));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::prepare_calc_split_dst(
          *ctx_->ls_handle_.get_ls(),
          *ctx_->main_tablet_handle_.get_obj(),
          ObTimeUtility::current_time() + (1 + ctx_->new_main_tablet_ids_.count()) * ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
          src_split_data,
          dst_split_datas))) {
    LOG_WARN("failed to prepare calc split dst", K(ret), K(ctx_->new_main_tablet_ids_));
  } else {
    ObLobIdMap* submap = ctx_->sub_maps_.at(task_id_);
    for (int64_t i = 0; i < iters.count() && OB_SUCC(ret); i++) {
      ObRowScan *curr = iters.at(i);
      while (OB_SUCC(ret)) {
        const ObDatumRow *tmp_row = nullptr;
        if (OB_FAIL(curr->get_next_row(tmp_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          LOG_DEBUG("scan get main tablet row", KPC(tmp_row));
        }
        // get rowkey from row
        ObDatumRowkey rk;
        // get dst lob tablet id
        ObTabletID dst_tablet_id;
        int64_t idx = -1;
        int64_t dst_idx = 0;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(tmp_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get next row is null", K(ret));
        } else if (OB_FAIL(rk.assign(tmp_row->storage_datums_, rk_cnt_))) {
          LOG_WARN("fail to assign rowkey", K(ret));
        } else if (OB_FAIL(src_split_data.calc_split_dst(ctx_->main_tablet_handle_.get_obj()->get_rowkey_read_info(), dst_split_datas, rk, dst_tablet_id, dst_idx))) {
          LOG_WARN("failed to calc split dst tablet", K(ret));
        } else {
          bool is_found = false;
          for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < ctx_->new_main_tablet_ids_.count(); i++) {
            if (ctx_->new_main_tablet_ids_.at(i) == dst_tablet_id) {
              dst_tablet_id = ctx_->new_lob_tablet_ids_.at(i);
              idx = i;
              is_found = true;
            }
          }
          if (!is_found) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not found dst main tablet in array", K(ret), K(dst_tablet_id), K(ctx_->new_main_tablet_ids_));
          }
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < param_->lob_col_idxs_.count(); i++) {
          // lob col must not be rowkey, so just add extra rowkey cnt
          const uint64_t lob_idx = param_->lob_col_idxs_.at(i) + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
          if (lob_idx >= tmp_row->get_column_count()) {
            LOG_TRACE("Tail-added column datum did not exist, whose default value must be in row", K(lob_idx), KPC(tmp_row));
          } else {
            ObStorageDatum &datum = tmp_row->storage_datums_[lob_idx];
            if (datum.is_nop() || datum.is_null()) {
              // do nothing
            } else {
              const ObLobCommon& lob_common = datum.get_lob_data();
              if (!lob_common.in_row_) {
                const ObLobData* lob_data = reinterpret_cast<const ObLobData*>(lob_common.buffer_);
                ObLobIdItem item;
                item.id_ = lob_data->id_;
                item.tablet_idx_ = idx;
                if (OB_FAIL(submap->add_item(item))) {
                  LOG_WARN("fail to add lob item into extern sortmap", K(ret));
                } else {
                  LOG_DEBUG("push lob id into map", K(item), K(submap), K(lob_idx));
                }
              }
            }
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(submap->do_sort(false))) {
      LOG_WARN("fail to do sort submap", K(ret));
    } else {
      LOG_INFO("finish submap sort", K(task_id_), K(param_->ori_lob_meta_tablet_id_), KPC(submap));
    }
  }
  return ret;
}




ObTabletLobMergeMapTask::ObTabletLobMergeMapTask()
  : ObITask(TASK_TYPE_LOB_MERGE_MAP), is_inited_(false), param_(nullptr), ctx_(nullptr)
{
}

ObTabletLobMergeMapTask::~ObTabletLobMergeMapTask()
{
}

int ObTabletLobMergeMapTask::init(ObLobSplitParam &param, ObLobSplitContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletLobBuildMapTask has already been inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param), K(ctx));
  } else {
    param_ = &param;
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletLobMergeMapTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
  const int64_t expire_timestamp = 0;
  const int64_t buf_limit = SORT_MEMORY_LIMIT;
  const uint64_t tenant_id = param_->tenant_id_;
  bool is_data_split_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobBuildMapTask has not been inited before", K(ret));
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (OB_SUCCESS != (ctx_->data_ret_)) {
    LOG_WARN("build map task has already failed", "ret", ctx_->data_ret_);
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->new_lob_tablet_ids_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (ctx_->sub_maps_.count() != param_->parallelism_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub map count is wrong", K(ret), K(ctx_->sub_maps_.count()), K(param_->parallelism_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->sub_maps_.count(); i++) {
      ObLobIdMap* submap = ctx_->sub_maps_.at(i);
      if (OB_ISNULL(submap)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("submap is null", K(ret), K(i));
      } else if (OB_FAIL(submap->transfer_final_sorted_fragment_iter(*(ctx_->total_map_)))) {
        LOG_WARN("fail to combine into total map", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx_->total_map_->do_sort(true))) {
      LOG_WARN("fail to sort total map", K(ret));
    } else {
      LOG_INFO("finish merge to total map", KPC(ctx_->total_map_));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

ObTabletLobWriteSSTableCtx::ObTabletLobWriteSSTableCtx()
  : table_key_(), data_seq_(0), merge_type_(MAJOR_MERGE), meta_(), dst_uncommitted_tx_id_arr_(), dst_major_snapshot_version_(-1)
{
}

ObTabletLobWriteSSTableCtx::~ObTabletLobWriteSSTableCtx()
{
}

int ObTabletLobWriteSSTableCtx::init(const ObSSTable &org_sstable, const int64_t major_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (!org_sstable.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(org_sstable));
  } else {
    ObSSTableMetaHandle meta_handle;
    table_key_ = org_sstable.get_key();
    if (OB_FAIL(org_sstable.get_meta(meta_handle))) {
      LOG_WARN("get sstable meta failed", K(ret));
    } else {
      meta_ = meta_handle.get_sstable_meta().get_basic_meta();
      data_seq_ = meta_handle.get_sstable_meta().get_sstable_seq();
      merge_type_ = org_sstable.is_major_sstable() ? compaction::MAJOR_MERGE : compaction::MINOR_MERGE;
      dst_uncommitted_tx_id_arr_.reset();
      dst_major_snapshot_version_ = major_snapshot_version;
    }
  }
  return ret;
}

int ObTabletLobWriteSSTableCtx::assign(const ObTabletLobWriteSSTableCtx &other)
{
  int ret = OB_SUCCESS;
  table_key_ = other.table_key_;
  data_seq_ = other.data_seq_;
  merge_type_ = other.merge_type_;
  meta_ = other.meta_;
  dst_major_snapshot_version_ = other.dst_major_snapshot_version_;
  if (OB_FAIL(dst_uncommitted_tx_id_arr_.assign(other.dst_uncommitted_tx_id_arr_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

ObTabletLobWriteDataTask::ObTabletLobWriteDataTask()
  : ObITask(TASK_TYPE_LOB_WRITE_DATA),
    allocator_("SplitLobWRow", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    is_inited_(false), task_id_(0),
    rk_cnt_(0), write_sstable_ctx_array_(), param_(nullptr), ctx_(nullptr)
{}

ObTabletLobWriteDataTask::~ObTabletLobWriteDataTask()
{}

int ObTabletLobWriteDataTask::init(const int64_t task_id, ObLobSplitParam &param, ObLobSplitContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletLobBuildMapTask has already been inited", K(ret));
  } else if (task_id < 0 || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id), K(param), K(ctx));
  } else {
    task_id_ = task_id;
    param_ = &param;
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletLobWriteDataTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  bool is_data_split_finished = false;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTabletSplitMdsUserData lob_meta_split_data;
  const ObStorageSchema *lob_meta_storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobBuildMapTask has not been inited before", K(ret));
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
  } else if (OB_SUCCESS != (ctx_->data_ret_)) {
    LOG_WARN("write data task has already failed", "ret", ctx_->data_ret_);
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->new_lob_tablet_ids_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(ctx_->lob_meta_tablet_handle_.get_obj()->ObITabletMdsInterface::get_tablet_status(
          share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(param_));
  } else if (OB_FAIL(ctx_->lob_meta_tablet_handle_.get_obj()->get_split_data(lob_meta_split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(lob_meta_split_data.get_storage_schema(lob_meta_storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret));
  } else if (OB_FAIL(ObTabletLobSplitUtil::generate_col_param(lob_meta_storage_schema, rk_cnt_))) {
    LOG_WARN("fail to generate col param", K(ret), K(task_id_));
  } else {
    const int64_t major_snapshot_version = user_data.start_split_commit_version_;
    ObDatumRange query_range;
    query_range.set_whole_range(); // no parallel, just set whole range
    ObArray<ObIStoreRowIteratorPtr> iters;
    ObArrayArray<ObWholeDataStoreDesc> data_desc_arr;
    ObArrayArray<ObMacroBlockWriter *> macro_block_writer_arr;
    ObArrayArray<ObSSTableIndexBuilder*> index_builders;
    if (OB_SUCC(ret) && (share::ObSplitSSTableType::SPLIT_BOTH == param_->split_sstable_type_
          || share::ObSplitSSTableType::SPLIT_MAJOR == param_->split_sstable_type_)) {
      if (!ctx_->skipped_split_major_keys_.empty()) {
        FLOG_INFO("no need to regenerate major sstable", K(ret), K(major_snapshot_version), K(ctx_->skipped_split_major_keys_));
      } else if (OB_FAIL(ObTabletLobSplitUtil::open_snapshot_scan_iters(param_,
                                                                 ctx_,
                                                                 param_->dest_schema_id_,
                                                                 ctx_->lob_meta_tablet_handle_,
                                                                 ctx_->lob_meta_table_store_iterator_,
                                                                 query_range,
                                                                 major_snapshot_version,
                                                                 iters,
                                                                 write_sstable_ctx_array_))) {
        LOG_WARN("fail to open iters", K(ret));
      }
    }
    if (OB_SUCC(ret) && (share::ObSplitSSTableType::SPLIT_BOTH == param_->split_sstable_type_
          || share::ObSplitSSTableType::SPLIT_MINOR == param_->split_sstable_type_)) {
      if (OB_FAIL(ObTabletLobSplitUtil::open_uncommitted_scan_iters(param_,
                                                                    ctx_,
                                                                    param_->dest_schema_id_,
                                                                    ctx_->lob_meta_tablet_handle_,
                                                                    ctx_->lob_meta_table_store_iterator_,
                                                                    query_range,
                                                                    major_snapshot_version,
                                                                    iters,
                                                                    write_sstable_ctx_array_))) {
        LOG_WARN("fail to open iters", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_sstable_writers_and_builders(*lob_meta_storage_schema, data_desc_arr, macro_block_writer_arr, index_builders))) {
      LOG_WARN("fail to prepare new tablet writers", K(ret));
    } else if (OB_FAIL(dispatch_rows(iters, macro_block_writer_arr))) {
      LOG_WARN("fail to dispatch write row to split dst tablet", K(ret));
    } else if (share::ObSplitSSTableType::SPLIT_BOTH == param_->split_sstable_type_) {
      if (OB_FAIL(create_sstables(index_builders, share::ObSplitSSTableType::SPLIT_MINOR))) {
        LOG_WARN("create sstable failed", K(ret));
      } else if (OB_FAIL(create_sstables(index_builders, share::ObSplitSSTableType::SPLIT_MAJOR))) {
        LOG_WARN("create sstable failed", K(ret));
      }
    } else if (OB_FAIL(create_sstables(index_builders, param_->split_sstable_type_))) {
      LOG_WARN("create sstable failed", K(ret));
    }
    // release sstable writer and index builders
    data_desc_arr.reset();
    release_sstable_writers_and_builders(macro_block_writer_arr, index_builders);
    // close row scan iters
    for (int64_t i = 0; i < iters.count(); i++) {
      ObIStoreRowIterator *iter = iters.at(i).iter_;
      if (OB_NOT_NULL(iter)) {
        iter->~ObIStoreRowIterator();
        ctx_->allocator_.free(iter);
      }
    }
    iters.reset();
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  ObTabletLobSplitDag *dag = static_cast<ObTabletLobSplitDag *>(get_dag());
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(dag) &&
    OB_SUCCESS != (tmp_ret = dag->report_lob_split_status())) {
    // do not override ret if it has already failed.
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    LOG_WARN("fail to report lob tablet split status", K(ret), K(tmp_ret));
  }
  return ret;
}

int ObTabletLobWriteDataTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIDag *tmp_dag = get_dag();
  ObTabletLobWriteDataTask *buildmap_task = nullptr;
  const int64_t next_task_id = task_id_ + 1;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletLobWriteDataTask has not been inited", K(ret));
  } else if (true || next_task_id >= param_->parallelism_) { // not allow para now
    ret = OB_ITER_END;
  } else if (OB_ISNULL(tmp_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(tmp_dag->alloc_task(buildmap_task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_FAIL(buildmap_task->init(next_task_id, *param_, *ctx_))) {
    LOG_WARN("fail to init lob build map task", K(ret));
  } else {
    next_task = buildmap_task;
    LOG_INFO("generate next lob build map task", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    if (OB_ITER_END != ret) {
      ctx_->data_ret_ = ret;
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::prepare_sstable_writers_and_builders(
  const ObStorageSchema &storage_schema,
  ObArrayArray<ObWholeDataStoreDesc>& data_desc_arr,
  ObArrayArray<ObMacroBlockWriter*>& slice_writers,
  ObArrayArray<ObSSTableIndexBuilder*>& index_builders)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(slice_writers.reserve(write_sstable_ctx_array_.count()))) {
    LOG_WARN("fail to reserve slice writer array", K(ret));
  } else if (OB_FAIL(index_builders.reserve(write_sstable_ctx_array_.count()))) {
    LOG_WARN("fail to reserve index builder array", K(ret));
  } else {
    for (int64_t i = 0; i < write_sstable_ctx_array_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(slice_writers.push_back(ObArray<ObMacroBlockWriter*>()))) {
        LOG_WARN("false to init slice_writers array", K(ret));
      } else if (OB_FAIL(index_builders.push_back(ObArray<ObSSTableIndexBuilder*>()))) {
        LOG_WARN("false to init index_builders array", K(ret));
      } else if (OB_FAIL(data_desc_arr.push_back(ObArray<ObWholeDataStoreDesc>()))) {
        LOG_WARN("false to init index_builders array", K(ret));
      }
    }
  }
  for (int64_t i = 0; i < write_sstable_ctx_array_.count() && OB_SUCC(ret); i++) {
    for (int64_t j = 0; j < ctx_->new_lob_tablet_ids_.count() && OB_SUCC(ret); j++) {
      ObWholeDataStoreDesc data_desc;
      ObMacroBlockWriter *new_slice_writer = nullptr;
      ObSSTableIndexBuilder *new_index_builder = nullptr;
      if (OB_FAIL(data_desc_arr.push_back(i, data_desc))) {
        LOG_WARN("fail to push index builder", K(ret));
      } else if (OB_FAIL(prepare_sstable_writer(write_sstable_ctx_array_.at(i),
                                          ctx_->new_lob_tablet_ids_.at(j),
                                          storage_schema,
                                          data_desc_arr.at(i, j),
                                          new_slice_writer,
                                          new_index_builder))) {
        LOG_WARN("fail to prepare sstable writers", K(ret), K(i), K(j));
      } else if (OB_FAIL(slice_writers.push_back(i, new_slice_writer))) {
        LOG_WARN("fail to push slice writer", K(ret));
      } else if (FALSE_IT(new_slice_writer = nullptr)) { // if slice writer has pushed into array, set null, freed outer
      } else if (OB_FAIL(index_builders.push_back(i, new_index_builder))) {
        LOG_WARN("fail to push index builder", K(ret));
      }
      if (OB_FAIL(ret)) {
        release_slice_writer(new_slice_writer);
        release_index_builder(new_index_builder);
      }
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::prepare_sstable_writer(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                                     const ObTabletID &new_tablet_id,
                                                     const ObStorageSchema &storage_schema,
                                                     ObWholeDataStoreDesc &data_desc,
                                                     ObMacroBlockWriter *&slice_writer,
                                                     ObSSTableIndexBuilder *&index_builder)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param_->tenant_id_;
  slice_writer = nullptr;
  index_builder = nullptr;
  if (!write_sstable_ctx.is_valid() || !new_tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid new tablet id", K(ret), K(write_sstable_ctx), K(new_tablet_id));
  } else if (OB_FAIL(prepare_sstable_index_builder(write_sstable_ctx,
                                                   new_tablet_id,
                                                   storage_schema,
                                                   index_builder))) {
    LOG_WARN("fail to prepare index builder", K(ret));
  } else if (OB_FAIL(prepare_sstable_macro_writer(write_sstable_ctx, new_tablet_id, storage_schema, data_desc, index_builder, slice_writer))) {
    LOG_WARN("fail to prepare macro writer", K(ret));
  }
  return ret;
}

int ObTabletLobWriteDataTask::prepare_sstable_macro_writer(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                                           const ObTabletID &new_tablet_id,
                                                           const ObStorageSchema &storage_schema,
                                                           ObWholeDataStoreDesc &data_desc,
                                                           ObSSTableIndexBuilder *index_builder,
                                                           ObMacroBlockWriter *&slice_writer)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMacroDataSeq macro_start_seq(0);
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!new_tablet_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid new tablet id", K(ret), K(new_tablet_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ctx_->ls_handle_, new_tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(new_tablet_id));
  } else if (OB_FAIL(macro_start_seq.set_sstable_seq(write_sstable_ctx.data_seq_))) {
    LOG_WARN("set sstable logical seq failed", K(ret));
  } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_id_))) {
    LOG_WARN("set parallel degree failed", K(ret));
  } else {
    const ObMergeType merge_type = write_sstable_ctx.merge_type_;
    const int64_t snapshot_version = write_sstable_ctx.get_version();
    const bool micro_index_clustered = ctx_->lob_meta_tablet_handle_.get_obj()->get_tablet_meta().micro_index_clustered_;
    ObMacroSeqParam macro_seq_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = macro_start_seq.macro_data_seq_;
    ObPreWarmerParam pre_warm_param;
    ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
    if (OB_FAIL(data_desc.init(true/*is_ddl*/, storage_schema,
                               param_->ls_id_,
                               new_tablet_id,
                               merge_type,
                               snapshot_version,
                               param_->data_format_version_,
                               micro_index_clustered,
                               tablet_handle.get_obj()->get_transfer_seq(),
                               write_sstable_ctx.table_key_.get_end_scn()))) {
      LOG_WARN("fail to init data store desc", K(ret), "dest_tablet_id", new_tablet_id, KPC(param_), KPC(ctx_));
    } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = index_builder)) {
    } else if (FALSE_IT(data_desc.get_static_desc().is_ddl_ = true)) {
    } else if (OB_ISNULL(buf = ctx_->allocator_.alloc(sizeof(ObMacroBlockWriter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else if (OB_FAIL(pre_warm_param.init(param_->ls_id_, new_tablet_id))) {
      LOG_WARN("failed to init pre warm param", K(ret), "ls_id", param_->ls_id_, K(new_tablet_id));
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(
                               data_desc.get_desc(),
                               object_cleaner))) {
      LOG_WARN("failed to get cleaner from data store desc", K(ret));
    } else if (FALSE_IT(slice_writer = new (buf) ObMacroBlockWriter())) {
    } else if (OB_FAIL(slice_writer->open(data_desc.get_desc(), macro_start_seq.get_parallel_idx(),
        macro_seq_param, pre_warm_param, *object_cleaner))) {
      LOG_WARN("open macro_block_writer failed", K(ret), K(data_desc));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != slice_writer) {
        slice_writer->~ObMacroBlockWriter();
        slice_writer = nullptr;
      }
      if (nullptr != buf) {
        ctx_->allocator_.free(buf);
        buf = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::prepare_sstable_index_builder(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                                            const ObTabletID &new_tablet_id,
                                                            const ObStorageSchema &storage_schema,
                                                            ObSSTableIndexBuilder *&index_builder)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObWholeDataStoreDesc data_desc;
  ObITable::TableKey dest_table_key = write_sstable_ctx.table_key_;
  dest_table_key.tablet_id_ = new_tablet_id;
  const ObMergeType merge_type = write_sstable_ctx.merge_type_;
  const int64_t snapshot_version = write_sstable_ctx.get_version();
  if (OB_UNLIKELY(!ctx_->lob_meta_tablet_handle_.is_valid())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("table is null", K(ret), K(ctx_->lob_meta_tablet_handle_));
  } else if (!new_tablet_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid new tablet id", K(ret), K(new_tablet_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ctx_->ls_handle_, new_tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(*tablet_handle.get_obj(),
                                                              dest_table_key,
                                                              snapshot_version,
                                                              param_->data_format_version_,
                                                              nullptr,
                                                              &storage_schema,
                                                              data_desc))) {
    LOG_WARN("prepare index data desc failed", K(ret));
  } else {
    void *builder_buf = nullptr;
    if (OB_UNLIKELY(!data_desc.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data store desc", K(ret), K(data_desc));
    } else if (OB_ISNULL(builder_buf = ctx_->allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (FALSE_IT(index_builder = new (builder_buf) ObSSTableIndexBuilder(false/*use double write buffer*/))) {
    } else if (OB_FAIL(index_builder->init(data_desc.get_desc(),
                                            ObSSTableIndexBuilder::DISABLE))) {
      LOG_WARN("failed to init index builder", K(ret), K(data_desc));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != index_builder) {
        index_builder->~ObSSTableIndexBuilder();
        index_builder = nullptr;
      }
      if (nullptr != builder_buf) {
        ctx_->allocator_.free(builder_buf);
        builder_buf = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::dispatch_rows(ObIArray<ObIStoreRowIteratorPtr>& iters,
                                            ObArrayArray<ObMacroBlockWriter*>& slice_writers)
{
  int ret = OB_SUCCESS;
  // do scan
  ObArray<bool> is_iter_finish;
  ObArray<const ObDatumRow *> cur_rows;
  if (OB_FAIL(cur_rows.reserve(iters.count()))) {
    LOG_WARN("fail to reserve ", K(ret));
  } else if (OB_FAIL(is_iter_finish.reserve(iters.count()))) {
    LOG_WARN("fail to reserve ", K(ret));
  } else {
    for (int64_t i = 0; i < iters.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(is_iter_finish.push_back(false))) {
        LOG_WARN("false to init is_iter_finish array", K(ret));
      } else if (OB_FAIL(cur_rows.push_back(nullptr))) {
        LOG_WARN("false to init cur_rows array", K(ret));
      }
    }
  }
  const ObLobIdItem *cur_item = nullptr;
  while (OB_SUCC(ret)) {
    // 1. total map and all iter do get next row if needed
    if (OB_ISNULL(cur_item)) {
      if (OB_FAIL(ctx_->total_map_->get_next_item(cur_item))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next total map item", K(ret));
        } // map return iter end to finish while
      } else {
        LOG_DEBUG("scan get next map item", KPC(cur_item));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < iters.count(); i++) {
      if (OB_ISNULL(cur_rows.at(i)) && !is_iter_finish.at(i)) {
        const ObDatumRow *tmp_row = nullptr;
        if (OB_FAIL(iters.at(i).iter_->get_next_row(tmp_row))) {
          if (ret == OB_ITER_END) {
            is_iter_finish.at(i) = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next row by iter", K(ret), K(i));
          }
        } else {
          cur_rows.at(i) = tmp_row;
        }
      }
    }
    // 2. traverse cur row, compare to cur map item
    bool hit_item = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < iters.count(); i++) {
      if (!is_iter_finish.at(i)) {
        const ObDatumRow *row = cur_rows.at(i);
        if (OB_ISNULL(cur_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item is null", K(ret));
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row is null", K(ret));
        } else {
          ObString data = row->storage_datums_[0].get_string();
          ObLobId *lob_id = reinterpret_cast<ObLobId*>(data.ptr());
          if (lob_id->lob_id_ == cur_item->id_.lob_id_ && lob_id->tablet_id_ == cur_item->id_.tablet_id_) {
            hit_item = true;
            LOG_DEBUG("scan lob meta row hit", KPC(row), KPC(lob_id));
            ObTabletID new_lob_tablet_id = ctx_->new_lob_tablet_ids_.at(cur_item->tablet_idx_);

            // fill uncommitted_tx_id for create sstable param
            if (OB_SUCC(ret) && is_minor_merge(write_sstable_ctx_array_.at(i).merge_type_) && row->is_uncommitted_row()) {
              ObIArray<int64_t> &tx_id_arr = write_sstable_ctx_array_.at(i).dst_uncommitted_tx_id_arr_;
              if (OB_UNLIKELY(!row->get_trans_id().is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("uncommitted row with invalid trans id", K(ret));
              } else if (OB_UNLIKELY(cur_item->tablet_idx_ >= tx_id_arr.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("index out of range", K(ret), K(tx_id_arr), KPC(cur_item));
              } else if (0 == tx_id_arr.at(cur_item->tablet_idx_)) {
                tx_id_arr.at(cur_item->tablet_idx_) = row->get_trans_id().get_id();
              }
            }

            ObMacroBlockWriter *slice_writer = slice_writers.at(i, cur_item->tablet_idx_);
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(slice_writer)) {
              ret = OB_ERR_NULL_VALUE;
              LOG_WARN("get null macro writer ptr", K(ret), K(i), KPC(cur_item));
            } else if (OB_FAIL(slice_writer->append_row(*row))) {
              LOG_WARN("append row failed", K(ret), K(new_lob_tablet_id));
            } else {
              LOG_DEBUG("write one row to new lob meta tablet", K(*lob_id), K(new_lob_tablet_id));
              // reset row pos
              cur_rows.at(i) = nullptr;
              ctx_->row_inserted_++;
            }
          } else {
            // check order
            ObLobIdItem src_item;
            src_item.id_ = *lob_id;
            bool src_item_small = ObLobIdItemCompare(ret)(&src_item, cur_item);
            if (OB_FAIL(ret)) {
            } else if (OB_UNLIKELY(src_item_small)) {
              ret = OB_ROWKEY_ORDER_ERROR;
              LOG_WARN("src lob meta row is smaller than current item", K(ret), K(*lob_id), KPC(cur_item), K(write_sstable_ctx_array_.at(i)));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && !hit_item) {
      // set cur_item nullptr to get next map item
      cur_item = nullptr;
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  // check all src row iters consumed
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < iters.count() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(cur_rows.at(i)) && !is_iter_finish.at(i)) {
        const ObDatumRow *tmp_row = nullptr;
        if (OB_FAIL(iters.at(i).iter_->get_next_row(tmp_row))) {
          if (ret == OB_ITER_END) {
            is_iter_finish.at(i) = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get next row by iter", K(ret), K(i));
          }
        } else {
          cur_rows.at(i) = tmp_row;
        }
      }
      if (OB_SUCC(ret) && nullptr != cur_rows.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("some lob row not dispatched", K(ret), K(i), K(cur_rows.at(i)), K(write_sstable_ctx_array_.at(i)));
      }
    }
  }

  // close macro writers
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_writers.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < slice_writers.count(i); j++) {
      if (OB_ISNULL(slice_writers.at(i, j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret));
      } else if (OB_FAIL(slice_writers.at(i, j)->close())) {
        LOG_WARN("close macro block writer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::create_sstables(
    ObArrayArray<ObSSTableIndexBuilder*>& index_builders,
    const share::ObSplitSSTableType split_sstable_type)
{
  int ret = OB_SUCCESS;
  int64_t last_minor_idx = -1;
  ObFixedArray<ObTablesHandleArray, common::ObIAllocator> batch_sstables_handle;
  batch_sstables_handle.set_allocator(&allocator_);
  const compaction::ObMergeType merge_type = share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type ?
        compaction::ObMergeType::MINOR_MERGE : compaction::ObMergeType::MAJOR_MERGE;
  common::ObArenaAllocator build_mds_arena("SplitBuildMds", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(batch_sstables_handle.prepare_allocate(ctx_->new_lob_tablet_ids_.count()))) {
    LOG_WARN("init failed", K(ret), K(ctx_->new_lob_tablet_ids_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < index_builders.count(); i++) {
    ObTabletLobWriteSSTableCtx &write_sstable_ctx = write_sstable_ctx_array_.at(i);
    if (is_minor_merge(write_sstable_ctx.merge_type_)) {
      last_minor_idx = i;
    }
  }

  for (int64_t i = 0; i < index_builders.count() && OB_SUCC(ret); i++) {
    for (int64_t j = 0; j < index_builders.count(i) && OB_SUCC(ret); j++) {
      bool is_data_split_finished = false;
      ObTableHandleV2 new_table_handle;
      const ObTabletID &dst_tablet_id = ctx_->new_lob_tablet_ids_.at(j);
      ObTabletLobWriteSSTableCtx &write_sstable_ctx = write_sstable_ctx_array_.at(i);
      ObSEArray<ObTabletID, 1> check_major_exist_tablets;
      if (OB_FAIL(check_major_exist_tablets.push_back(dst_tablet_id))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, check_major_exist_tablets, is_data_split_finished))) {
        LOG_WARN("check major exist failed", K(ret), K(check_major_exist_tablets), KPC(param_));
      } else if (is_data_split_finished) {
        FLOG_INFO("skip to create sstable", K(ret), K(dst_tablet_id));
      } else if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type
          && !is_minor_merge(write_sstable_ctx.merge_type_)) {
        LOG_DEBUG("skip this sstable", K(split_sstable_type), K(write_sstable_ctx));
      } else if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type
          && !is_major_merge(write_sstable_ctx.merge_type_)) {
        LOG_DEBUG("skip this sstable", K(split_sstable_type), K(write_sstable_ctx));
      } else if (OB_FAIL(create_sstable(index_builders.at(i, j),
                                        write_sstable_ctx,
                                        j,
                                        dst_tablet_id,
                                        new_table_handle))) {
        LOG_WARN("fail to create sstable", K(ret));
      } else if (OB_FAIL(batch_sstables_handle.at(j).add_table(new_table_handle))) {
        LOG_WARN("fail to push back new sstable handle", K(ret));
      } else if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type
          && i == last_minor_idx) {
        // fill empty minor sstable if scn not contiguous
        bool need_fill_empty_sstable = false;
        SCN end_scn;
        if (OB_FAIL(ObTabletSplitMergeTask::check_need_fill_empty_sstable(ctx_->ls_handle_,
                                                                          is_minor_merge(write_sstable_ctx.merge_type_),
                                                                          write_sstable_ctx.table_key_,
                                                                          dst_tablet_id,
                                                                          need_fill_empty_sstable,
                                                                          end_scn))) {
          LOG_WARN("failed to check need fill", K(ret));
        } else if (need_fill_empty_sstable) {
          new_table_handle.reset();
          if (OB_FAIL(create_empty_sstable(write_sstable_ctx, dst_tablet_id, end_scn, new_table_handle))) {
            LOG_WARN("failed to create empty sstable", K(ret));
          } else if (OB_FAIL(batch_sstables_handle.at(j).add_table(new_table_handle))) {
            LOG_WARN("fail to push back new sstable handle", K(ret));
          }
        } else {
          LOG_INFO("no need fill empty sstable", K(dst_tablet_id), K(write_sstable_ctx));
        }
      }
    }
  }

  // update tablet handle table store
  for (int64_t i = 0; i < ctx_->new_lob_tablet_ids_.count() && OB_SUCC(ret); i++) {
    if (batch_sstables_handle.at(i).empty() && !is_major_merge_type(merge_type)) {
      FLOG_INFO("already built, skip to update table store", K(ret), "tablet_id", ctx_->new_lob_tablet_ids_.at(i));
    } else if (OB_FAIL(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
                ctx_->ls_handle_,
                ctx_->lob_meta_tablet_handle_,
                ctx_->new_lob_tablet_ids_.at(i),
                batch_sstables_handle.at(i),
                merge_type,
                false/*can_reuse_macro_block*/,
                ctx_->skipped_split_major_keys_))) {
      LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle.at(i)), K(split_sstable_type));
    }
    if (OB_SUCC(ret) && !is_major_merge_type(merge_type)) {
      // build lost mds sstable into tablet.
      ObTableHandleV2 mds_table_handle;
      ObTablesHandleArray mds_sstables_handle;
      if (OB_FAIL(ObTabletSplitUtil::build_lost_medium_mds_sstable(
            build_mds_arena,
            ctx_->ls_handle_,
            ctx_->lob_meta_tablet_handle_,
            ctx_->new_lob_tablet_ids_.at(i),
            mds_table_handle))) {
        LOG_WARN("build lost medium mds sstable failed", K(ret), KPC(param_));
      } else if (OB_UNLIKELY(!mds_table_handle.is_valid())) {
        LOG_INFO("no need to fill medium mds sstable", K(ret), KPC(param_));
      } else if (OB_FAIL(mds_sstables_handle.add_table(mds_table_handle))) {
        LOG_WARN("add table failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
            ctx_->ls_handle_,
            ctx_->lob_meta_tablet_handle_,
            ctx_->new_lob_tablet_ids_.at(i),
            mds_sstables_handle,
            compaction::ObMergeType::MDS_MINI_MERGE,
            false/*can_reuse_macro_block*/,
            ctx_->skipped_split_major_keys_))) {
        LOG_WARN("update table store with batch tables failed", K(ret), K(mds_sstables_handle));
      }
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::create_empty_sstable(const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                                   const ObTabletID &new_tablet_id,
                                                   const SCN &end_scn,
                                                   ObTableHandleV2 &new_table_handle)
{
  int ret = OB_SUCCESS;
  new_table_handle.reset();
  ObTabletCreateSSTableParam create_sstable_param;
  if (OB_FAIL(ObTabletSplitMergeTask::build_create_empty_sstable_param(write_sstable_ctx.meta_, write_sstable_ctx.table_key_, new_tablet_id, end_scn, create_sstable_param))) {
    LOG_WARN("failed to build create empty sstable param", K(ret));
  } else {
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, ctx_->allocator_, new_table_handle))) {
      LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
    }
  }
  return ret;
}

int ObTabletLobWriteDataTask::create_sstable(ObSSTableIndexBuilder *sstable_index_builder,
                                             const ObTabletLobWriteSSTableCtx &write_sstable_ctx,
                                             const int64_t tablet_idx,
                                             const ObTabletID &new_tablet_id,
                                             ObTableHandleV2 &new_table_handle)
{
  int ret = OB_SUCCESS;
  const ObITable::TableKey &table_key = write_sstable_ctx.table_key_;
  const ObSSTableBasicMeta &basic_meta = write_sstable_ctx.meta_;
  const compaction::ObMergeType &merge_type = write_sstable_ctx.merge_type_;
  const int64_t dst_major_snapshot_version = write_sstable_ctx.dst_major_snapshot_version_;
  new_table_handle.reset();
  SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, create_sstable_param)) {
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_ISNULL(sstable_index_builder)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg, index builder is null", K(ret));
    } else if (!basic_meta.is_valid() || !table_key.is_valid() || !new_tablet_id.is_valid()
        || tablet_idx < 0 || (is_minor_merge(write_sstable_ctx.merge_type_) && tablet_idx >= write_sstable_ctx.dst_uncommitted_tx_id_arr_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(tablet_idx), K(write_sstable_ctx));
    } else if (OB_FAIL(sstable_index_builder->close(res))) {
      LOG_WARN("close sstable index builder failed", K(ret));
    } else {
      const int64_t uncommitted_tx_id = is_minor_merge(merge_type) ? write_sstable_ctx.dst_uncommitted_tx_id_arr_.at(tablet_idx) : 0;
      if (OB_FAIL(create_sstable_param.init_for_lob_split(new_tablet_id, table_key, basic_meta, merge_type,
          param_->schema_version_, dst_major_snapshot_version, uncommitted_tx_id, write_sstable_ctx.data_seq_, res))) {
        LOG_WARN("init sstable param failed", K(ret), K(new_tablet_id), K(table_key), K(basic_meta), K(merge_type), K(res));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, ctx_->allocator_, new_table_handle))) {
        LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
      }
    }
  }
  return ret;
}

void ObTabletLobWriteDataTask::release_slice_writer(ObMacroBlockWriter *&slice_writer)
{
  if (OB_NOT_NULL(slice_writer)) {
    slice_writer->~ObMacroBlockWriter();
    ctx_->allocator_.free(slice_writer);
    slice_writer = nullptr;
  }
}

void ObTabletLobWriteDataTask::release_index_builder(ObSSTableIndexBuilder *&index_builder)
{
  if (nullptr != index_builder) {
    index_builder->~ObSSTableIndexBuilder();
    ctx_->allocator_.free(index_builder);
    index_builder = nullptr;
  }
}

void ObTabletLobWriteDataTask::release_sstable_writers_and_builders(
  ObArrayArray<ObMacroBlockWriter*>& slice_writers,
  ObArrayArray<ObSSTableIndexBuilder*>& index_builders)
{
  for (int64_t i = 0; i < slice_writers.count(); i++) {
    for (int64_t j = 0; j < slice_writers.count(i); j++) {
      release_slice_writer(slice_writers.at(i, j));
    }
  }
  slice_writers.reset();
  for (int64_t i = 0; i < index_builders.count(); i++) {
    for (int64_t j = 0; j < index_builders.count(i); j++) {
      release_index_builder(index_builders.at(i, j));
    }
  }
  index_builders.reset();
}

/*****************************ObTabletLobWriteDataTask END****************************************/

int ObTabletLobSplitUtil::open_rowscan_iters(const share::ObSplitSSTableType &split_sstable_type,
                                             ObIAllocator &allocator,
                                             int64_t table_id,
                                             const ObTabletHandle &tablet_handle,
                                             const ObTableStoreIterator &const_iterator,
                                             const ObDatumRange &query_range,
                                             const ObStorageSchema &main_table_storage_schema,
                                             ObIArray<ObRowScan*>& iters)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator iterator;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpecter error", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (OB_FAIL(iterator.assign(const_iterator))) {
    LOG_WARN("failed to assign iterator", K(ret));
  } else {
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iterator.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tables", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(iterator));
      } else if (!table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be sstable", K(ret), KPC(table), K(iterator));
      } else if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type
          && table->is_minor_sstable()) {
        LOG_DEBUG("ignore to build the minor", KPC(table));
      } else if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type
          && table->is_major_sstable()) {
        LOG_DEBUG("ignore to build the major", KPC(table));
      } else if (table->is_mds_sstable()) {
        LOG_DEBUG("ignore to build mds sstable", KPC(table));
      } else {
        LOG_INFO("open one sstable rowscan", KPC(table));
        ObSSTable *sst = static_cast<ObSSTable*>(table);
        // preprare row scan iter
        if (OB_SUCC(ret)) {
          ObSplitScanParam scan_param(table_id, *tablet, query_range, main_table_storage_schema);
          ObRowScan *new_scanner = nullptr;
          void* buff = allocator.alloc(sizeof(ObRowScan));
          if (OB_ISNULL(buff)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc row scan", K(ret));
          } else if (FALSE_IT(new_scanner = new(buff)ObRowScan())) {
          } else if (OB_FAIL(new_scanner->init(scan_param, *sst))) {
            LOG_WARN("fail to init row scanner", K(ret));
          } else if (OB_FAIL(iters.push_back(new_scanner))) {
            LOG_WARN("fail to push back new row scanner", K(ret));
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(new_scanner)) {
            new_scanner->~ObRowScan();
            allocator.free(new_scanner);
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTabletLobSplitUtil::open_uncommitted_scan_iters(ObLobSplitParam *param,
                                                      ObLobSplitContext *ctx,
                                                      int64_t table_id,
                                                      const ObTabletHandle &tablet_handle,
                                                      const ObTableStoreIterator &const_table_iter,
                                                      const ObDatumRange &query_range,
                                                      const int64_t major_snapshot_version,
                                                      ObIArray<ObIStoreRowIteratorPtr> &iters,
                                                      ObIArray<ObTabletLobWriteSSTableCtx> &write_sstable_ctx_array)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_iter;
  ObTablet *tablet = nullptr;
  ObTabletSplitMdsUserData lob_meta_split_data;
  const ObStorageSchema *lob_meta_storage_schema = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null param or null ctx", K(ret), K(param), K(ctx));
  } else if (!param->is_valid() || !ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split param", K(ret), K(*param), K(*ctx));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpecter error", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (OB_FAIL(table_iter.assign(const_table_iter))) {
    LOG_WARN("failed to assign iterator", K(ret));
  } else if (OB_FAIL(tablet->get_split_data(lob_meta_split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(lob_meta_split_data.get_storage_schema(lob_meta_storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret));
  } else {
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tables", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(table_iter));
      } else if (!table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be sstable", K(ret), KPC(table), K(table_iter));
      } else if (table->is_minor_sstable()) {
        LOG_INFO("open one sstable rowscan", KPC(table));
        ObTabletLobWriteSSTableCtx write_sstable_ctx;
        ObSSTable *sst = static_cast<ObSSTable*>(table);
        if (OB_FAIL(write_sstable_ctx.init(*sst, major_snapshot_version))) {
          LOG_WARN("init write sstable ctx failed", K(ret));
        } else if (OB_FAIL(write_sstable_ctx.dst_uncommitted_tx_id_arr_.prepare_allocate(ctx->new_lob_tablet_ids_.count(), 0))) {
          LOG_WARN("failed to prepare allocate", K(ret), K(ctx->new_lob_tablet_ids_));
        } else if (OB_FAIL(write_sstable_ctx_array.push_back(write_sstable_ctx))) {
          LOG_WARN("push back write sstable ctx failed", K(ret));
        } else {
          ObSplitScanParam scan_param(table_id, *tablet, query_range, *lob_meta_storage_schema);
          ObUncommittedRowScan *new_scanner = nullptr;
          void *buff = ctx->allocator_.alloc(sizeof(ObUncommittedRowScan));
          if (OB_ISNULL(buff)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc row scan", K(ret));
          } else if (FALSE_IT(new_scanner = new(buff)ObUncommittedRowScan())) {
          } else if (OB_FAIL(new_scanner->init(scan_param, *sst, major_snapshot_version, ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
            LOG_WARN("fail to init row scanner", K(ret));
          } else if (OB_FAIL(iters.push_back(ObIStoreRowIteratorPtr(new_scanner)))) {
            LOG_WARN("fail to push back new row scanner", K(ret));
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(new_scanner)) {
            new_scanner->~ObUncommittedRowScan();
            ctx->allocator_.free(new_scanner);
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTabletLobSplitUtil::open_snapshot_scan_iters(ObLobSplitParam *param,
                                                   ObLobSplitContext *ctx,
                                                   int64_t table_id,
                                                   const ObTabletHandle &tablet_handle,
                                                   const ObTableStoreIterator &const_table_iter,
                                                   const ObDatumRange &query_range,
                                                   const int64_t major_snapshot_version,
                                                   ObIArray<ObIStoreRowIteratorPtr> &iters,
                                                   ObIArray<ObTabletLobWriteSSTableCtx> &write_sstable_ctx_array)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_iter;
  ObTableHandleV2 last_major_table_handle;
  ObTablet *tablet = nullptr;
  ObTabletSplitMdsUserData lob_meta_split_data;
  const ObStorageSchema *lob_meta_storage_schema = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null param or null ctx", K(ret), K(param), K(ctx));
  } else if (!param->is_valid() || !ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split param", K(ret), K(*param), K(*ctx));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpecter error", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_split_data(lob_meta_split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(lob_meta_split_data.get_storage_schema(lob_meta_storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret));
  } else if (OB_FAIL(table_iter.assign(const_table_iter))) {
    LOG_WARN("failed to assign iterator", K(ret));
  } else {
    ObTableHandleV2 table_handle;
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(table_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tables", K(ret));
        }
      } else if (OB_UNLIKELY(!table_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(table_handle), K(table_iter));
      } else if (OB_FALSE_IT(table = table_handle.get_table())) {
      } else if (!table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must be sstable", K(ret), KPC(table), K(table_iter));
      } else if (table->is_major_sstable()) {
        if (!last_major_table_handle.is_valid() || table->get_snapshot_version() > last_major_table_handle.get_table()->get_snapshot_version()) {
          last_major_table_handle = table_handle;
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!last_major_table_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid last major table", K(ret), K(last_major_table_handle), K(table_iter));
  } else {
    HEAP_VAR(ObTableSchema, aux_lob_meta_schema) {
    LOG_INFO("open one sstable rowscan", K(last_major_table_handle));
    const bool is_oracle_mode = lib::Worker::CompatMode::ORACLE == param->compat_mode_;
    ObArray<ObColDesc> col_descs;
    ObTabletLobWriteSSTableCtx write_sstable_ctx;
    ObSSTable *sst = static_cast<ObSSTable*>(last_major_table_handle.get_table());
    if (OB_FAIL(write_sstable_ctx.init(*sst, major_snapshot_version))) {
      LOG_WARN("init write sstable ctx failed", K(ret));
    } else if (OB_FAIL(write_sstable_ctx_array.push_back(write_sstable_ctx))) {
      LOG_WARN("push back write sstable ctx failed", K(ret));
    } else if (OB_FAIL(ObInnerTableSchema::all_column_aux_lob_meta_schema(aux_lob_meta_schema))) {
      LOG_WARN("get lob meta schema failed", K(ret));
    } else if (OB_FAIL(aux_lob_meta_schema.get_store_column_ids(col_descs))) {
      LOG_WARN("failed to get store column ids", K(ret));
    } else {
      ObSplitScanParam scan_param(table_id, *tablet, query_range, *lob_meta_storage_schema);
      ObSnapshotRowScan *new_scanner = nullptr;
      void *buff = ctx->allocator_.alloc(sizeof(ObSnapshotRowScan));
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc row scan", K(ret));
      } else if (FALSE_IT(new_scanner = new(buff)ObSnapshotRowScan())) {
      } else if (OB_FAIL(new_scanner->init(scan_param,
                                           col_descs,
                                           aux_lob_meta_schema.get_column_count(),
                                           aux_lob_meta_schema.get_rowkey_column_num(),
                                           is_oracle_mode,
                                           tablet_handle,
                                           major_snapshot_version))) {
        LOG_WARN("fail to init row scanner", K(ret));
      } else if (OB_FAIL(iters.push_back(ObIStoreRowIteratorPtr(new_scanner)))) {
        LOG_WARN("fail to push back new row scanner", K(ret));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(new_scanner)) {
        new_scanner->~ObSnapshotRowScan();
        ctx->allocator_.free(new_scanner);
      }
    }
    }
  }
  return ret;
}


int ObTabletLobSplitUtil::generate_col_param(const ObMergeSchema *schema,
                                             int64_t& rk_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is null", K(ret));
  } else {
    rk_cnt = schema->get_rowkey_column_num();
  }
  return ret;
}

int ObTabletLobSplitUtil::process_write_split_start_log_request(
    const ObTabletSplitArg &arg,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObLobSplitParam lob_split_param;
  ObTabletSplitParam data_split_param;
  const share::ObLSID &ls_id = arg.ls_id_;
  const bool is_start_request = true;
  const bool is_lob_tablet = arg.lob_col_idxs_.count() > 0;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (is_lob_tablet) {
    if (OB_FAIL(lob_split_param.init(arg))) {
      LOG_WARN("init param failed", K(ret));
    } else if (OB_FAIL(ObTabletLobSplitUtil::write_split_log(is_lob_tablet, is_start_request, ls_id, &lob_split_param, scn))) {
      LOG_WARN("write split log failed", K(ret));
    }
  } else {
    if (OB_FAIL(data_split_param.init(arg))) {
      LOG_WARN("init param failed", K(ret));
    } else if (OB_FAIL(ObTabletLobSplitUtil::write_split_log(is_lob_tablet, is_start_request, ls_id, &data_split_param, scn))) {
      LOG_WARN("write split log failed", K(ret));
    }
  }
  LOG_INFO("write tablet split log finish", K(ret), K(is_lob_tablet), K(lob_split_param), K(data_split_param));
  return ret;
}


int ObTabletLobSplitUtil::process_tablet_split_request(
    const bool is_lob_tablet,
    const bool is_start_request,
    const void *request_arg,
    void *request_res)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id;
  ObLobSplitParam lob_split_param;
  ObTabletSplitParam data_split_param;
  share::ObIDagInitParam *dag_param = nullptr;

  ObIDag *stored_dag = nullptr;
  ObTabletLobSplitDag *lob_split_dag = nullptr;
  ObTabletSplitDag *data_split_dag = nullptr;
  if (OB_UNLIKELY(nullptr == request_arg || nullptr == request_res)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(request_arg), KP(request_res));
  } else if (is_lob_tablet) {
    if (is_start_request && OB_FAIL(lob_split_param.init(*static_cast<const ObDDLBuildSingleReplicaRequestArg *>(request_arg)))) {
      LOG_WARN("init param failed", K(ret));
    } else if (!is_start_request && OB_FAIL(lob_split_param.init(*static_cast<const obrpc::ObTabletSplitArg *>(request_arg)))) {
      LOG_WARN("init param failed", K(ret));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_and_get_lob_tablet_split_dag(lob_split_param, lob_split_dag))) {
      LOG_WARN("failed to schedule dag", K(ret));
    }
    ls_id = lob_split_param.ls_id_;
    dag_param = &lob_split_param;
    stored_dag = lob_split_dag;
  } else {
    // data tablet, index tablet.
    if (is_start_request && OB_FAIL(data_split_param.init(*static_cast<const ObDDLBuildSingleReplicaRequestArg *>(request_arg)))) {
      LOG_WARN("init param failed", K(ret));
    } else if (!is_start_request && OB_FAIL(data_split_param.init(*static_cast<const obrpc::ObTabletSplitArg *>(request_arg)))) {
      LOG_WARN("init param failed", K(ret));
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_and_get_tablet_split_dag(data_split_param, data_split_dag))) {
      LOG_WARN("failed to schedule dag", K(ret));
    }
    ls_id = data_split_param.ls_id_;
    dag_param = &data_split_param;
    stored_dag = data_split_dag;
  }

  if (OB_SUCC(ret) && !is_start_request) {
    share::SCN unused_finish_scn = SCN::min_scn();
    if (OB_FAIL(ObTabletLobSplitUtil::write_split_log(is_lob_tablet, is_start_request, ls_id, dag_param, unused_finish_scn))) {
      LOG_WARN("write split log failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // try add dag, and get progress.
    // FIXME (YIREN), to return progress.
    int64_t unused_row_inserted = 0;
    int64_t unused_phy_row_cnt = 0;
    int64_t &row_inserted = is_start_request ?
        static_cast<ObDDLBuildSingleReplicaRequestResult*>(request_res)->row_inserted_ : unused_row_inserted;
    int64_t &physical_row_count = is_start_request ?
        static_cast<ObDDLBuildSingleReplicaRequestResult*>(request_res)->physical_row_count_ : unused_phy_row_cnt;
    if (is_lob_tablet) {
      if (OB_FAIL(add_dag_and_get_progress<ObTabletLobSplitDag>(lob_split_dag, row_inserted, physical_row_count))) {
        if (OB_EAGAIN == ret) { // dag exists.
          ret = OB_SUCCESS;
        } else if (OB_SIZE_OVERFLOW == ret) { // add dag failed.
          ret = OB_EAGAIN;
        } else {
          LOG_WARN("add dag and get progress failed", K(ret));
        }
      } else {
        stored_dag = nullptr; // to avoid free.
      }
    } else {
      if (OB_FAIL(add_dag_and_get_progress<ObTabletSplitDag>(data_split_dag, row_inserted, physical_row_count))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_EAGAIN;
        } else {
          LOG_WARN("add dag and get progress failed", K(ret));
        }
      } else {
        stored_dag = nullptr; // to avoid free.
      }
    }
  }

  if (OB_NOT_NULL(stored_dag)) {
    // to free dag.
    MTL(ObTenantDagScheduler*)->free_dag(*stored_dag);
    stored_dag = nullptr;
  }
  LOG_INFO("process tablet split request finish", K(ret), K(is_start_request), K(is_lob_tablet), K(lob_split_param), K(data_split_param),
    K(data_split_dag), K(lob_split_dag));
  return ret;
}

int ObTabletLobSplitUtil::write_split_log(
    const bool is_lob_tablet,
    const bool is_start_request,
    const share::ObLSID &ls_id,
    const share::ObIDagInitParam *input_param,
    SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTabletSplitStartLog split_start_log;
  ObTabletSplitFinishLog split_finish_log;
  ObTabletSplitInfo &split_info = is_start_request ? split_start_log.basic_info_ : split_finish_log.basic_info_;
  if (OB_UNLIKELY(!ls_id.is_valid() || nullptr == input_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), KP(input_param));
  } else if (is_lob_tablet) {
    const ObLobSplitParam *param = static_cast<const ObLobSplitParam *>(input_param);
    if (OB_UNLIKELY(!param->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), KPC(param));
    } else {
      split_info.table_id_               = param->source_table_id_; // rely on it to scan data table rows.
      split_info.lob_table_id_           = param->dest_schema_id_;
      split_info.schema_version_         = param->schema_version_;
      split_info.task_id_                = param->task_id_;
      split_info.source_tablet_id_       = param->ori_lob_meta_tablet_id_;
      split_info.compaction_scn_         = param->compaction_scn_;
      split_info.data_format_version_    = param->data_format_version_;
      split_info.consumer_group_id_      = param->consumer_group_id_;
      split_info.can_reuse_macro_block_  = false;
      split_info.split_sstable_type_     = param->split_sstable_type_;
      if (OB_FAIL(split_info.lob_col_idxs_.assign(param->lob_col_idxs_))) {
        LOG_WARN("assign failed", K(ret));
      } else if (OB_FAIL(split_info.parallel_datum_rowkey_list_.assign(param->parallel_datum_rowkey_list_))) {
        LOG_WARN("assign failed", K(ret), KPC(param));
      } else if (OB_FAIL(split_info.dest_tablets_id_.assign(param->new_lob_tablet_ids_))) {
        LOG_WARN("assign failed", K(ret), KPC(param));
      }
    }
  } else {
    const ObTabletSplitParam *param = static_cast<const ObTabletSplitParam *>(input_param);
    if (OB_UNLIKELY(!param->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), KPC(param));
    } else {
      split_info.table_id_               = param->table_id_;
      split_info.lob_table_id_           = OB_INVALID_ID;
      split_info.schema_version_         = param->schema_version_;
      split_info.task_id_                = param->task_id_;
      split_info.source_tablet_id_       = param->source_tablet_id_;
      split_info.compaction_scn_         = param->compaction_scn_;
      split_info.data_format_version_    = param->data_format_version_;
      split_info.consumer_group_id_      = param->consumer_group_id_;
      split_info.can_reuse_macro_block_  = param->can_reuse_macro_block_;
      split_info.split_sstable_type_     = param->split_sstable_type_;
      // skip lob col idxs.
      if (OB_FAIL(split_info.parallel_datum_rowkey_list_.assign(param->parallel_datum_rowkey_list_))) {
        LOG_WARN("assign failed", K(ret), KPC(param));
      } else if (OB_FAIL(split_info.dest_tablets_id_.assign(param->dest_tablets_id_))) {
        LOG_WARN("assign failed", K(ret), KPC(param));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_start_request) {
      if (OB_FAIL(ObDDLRedoLogWriter::write_auto_split_log(
            ls_id, ObDDLClogType::DDL_TABLET_SPLIT_START_LOG,
            logservice::ObReplayBarrierType::PRE_BARRIER, split_start_log, scn))) {
        LOG_WARN("write tablet split start log failed", K(ret), K(split_start_log));
      }
    } else {
      if (OB_FAIL(ObDDLRedoLogWriter::write_auto_split_log(
            ls_id, ObDDLClogType::DDL_TABLET_SPLIT_FINISH_LOG,
            logservice::ObReplayBarrierType::STRICT_BARRIER, split_finish_log, scn))) {
        LOG_WARN("write tablet split finish log failed", K(ret), K(split_finish_log));
      }
    }
    LOG_INFO("write split log finish", K(ret), K(is_start_request), K(is_lob_tablet), K(split_start_log), K(split_finish_log));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
