//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_BATCH_EXEC_DAG_H_
#define OB_STORAGE_COMPACTION_BATCH_EXEC_DAG_H_
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "lib/lock/ob_mutex.h"
#include "storage/compaction/ob_sstable_merge_history.h"
namespace oceanbase
{
namespace compaction
{
enum ObBatchExecParamType : uint8_t
{
  BATCH_FREEZE = 0,
  VERIFY_CKM,
  UPDATE_SKIP_MAJOR_TABLET
};

template<typename ITEM>
struct ObBatchExecParam : public share::ObIDagInitParam
{
public:
  ObBatchExecParam(const ObBatchExecParamType type);
  ObBatchExecParam(
      const ObBatchExecParamType type,
      const share::ObLSID &ls_id,
      const int64_t merge_version,
      const int64_t batch_size = DEFAULT_BATCH_SIZE);
  virtual ~ObBatchExecParam() { tablet_info_array_.reset(); }
  bool is_valid() const override { return ls_id_.is_valid() && compaction_scn_ > 0 && tablet_info_array_.count() > 0; }
  int assign(const ObBatchExecParam &other);
  int64_t get_hash() const;
  int64_t get_task_cnt() const
  {
    int64_t ret_val = 1;
    if (batch_size_ > 0 && tablet_info_array_.count() > 0) {
      ret_val = (tablet_info_array_.count() + batch_size_ - 1)/ batch_size_;
    }
    return ret_val;
  }
  VIRTUAL_TO_STRING_KV(K_(param_type), K_(ls_id), K_(compaction_scn), "tablet_id_cnt", tablet_info_array_.count(), K_(tablet_info_array));
public:
  static constexpr int64_t DEFAULT_BATCH_SIZE = 128;
  static constexpr int64_t DEFAULT_ARRAY_SIZE = 64;
  ObBatchExecParamType param_type_;
  share::ObLSID ls_id_;
  int64_t compaction_scn_;
  int64_t batch_size_;
  common::ObSEArray<ITEM, DEFAULT_ARRAY_SIZE> tablet_info_array_;
};

struct ObBatchExecInfo
{
  ObBatchExecInfo()
    : success_cnt_(0),
      failure_cnt_(0),
      errno_(0)
  {}
  void add(const ObBatchExecInfo &input_info)
  {
    success_cnt_ += input_info.success_cnt_;
    failure_cnt_ += input_info.failure_cnt_;
    if (0 == errno_ && 0 != input_info.errno_) {
      errno_ = input_info.errno_;
    }
  }
  TO_STRING_KV(K_(success_cnt), K_(failure_cnt), K_(errno));

  int64_t success_cnt_;
  int64_t failure_cnt_;
  int errno_;
};

struct ObBatchExecCollector
{
  ObBatchExecCollector()
    : remain_task_cnt_(0),
      info_(),
      merge_history_()
  {}
  TO_STRING_KV(K_(remain_task_cnt), K_(info));
  void add(const ObBatchExecInfo input_info, ObIDag &dag);

  lib::ObMutex lock_;
  int64_t remain_task_cnt_;
  ObBatchExecInfo info_;
  ObSSTableMergeHistory merge_history_;
};

template<typename TASK, typename PARAM>
class ObBatchExecDag : public share::ObIDag
{
public:
  ObBatchExecDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObBatchExecDag() {}
  int init_by_param(const share::ObIDagInitParam *param);
  virtual int create_first_task() override;
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override { return param_.get_hash(); }
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  const PARAM &get_param() const { return param_; }
  void add_collect_cnt(const ObBatchExecInfo &cnt) { collector_.add(cnt, *this); }
  int init_merge_history();
  INHERIT_TO_STRING_KV("ObBatchExecDag", ObIDag, K_(is_inited), K_(param));
protected:
  ObBatchExecCollector collector_;
private:
  bool is_inited_;
  PARAM param_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchExecDag);
};


template<typename TASK, typename PARAM>
class ObBatchExecTask : public share::ObITask
{
public:
  ObBatchExecTask(const ObITaskType type)
    : ObITask(type),
      is_inited_(false),
      base_dag_(nullptr),
      idx_(0),
      cnt_()
  {}
  virtual ~ObBatchExecTask() {}
  int init(const int64_t input_idx);
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int process() override;
  virtual int inner_process() = 0;
protected:
  int64_t get_batch_size() const { return base_dag_->get_param().batch_size_; }
  int64_t get_start_idx() const { return idx_ * get_batch_size(); }
  int64_t get_end_idx() const { return (idx_ + 1) * get_batch_size(); }
protected:
  // execute ObBatchExecParam::tablet_info_array_ [idx_ * BATCH_SIZE, (idx_ + 1) * BATCH_SIZE)
  bool is_inited_;
  ObBatchExecDag<TASK, PARAM> *base_dag_;
  int64_t idx_;
  ObBatchExecInfo cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchExecTask);
};


/*
 *  ----------------------------------------ObBatchExecParam--------------------------------------------
 */
template<typename ITEM>
ObBatchExecParam<ITEM>::ObBatchExecParam(const ObBatchExecParamType type)
  : param_type_(type),
    ls_id_(),
    compaction_scn_(0),
    batch_size_(DEFAULT_BATCH_SIZE),
    tablet_info_array_()
{
  tablet_info_array_.set_attr(lib::ObMemAttr(MTL_ID(), "BatchArr", ObCtxIds::MERGE_NORMAL_CTX_ID));
}

template<typename ITEM>
ObBatchExecParam<ITEM>::ObBatchExecParam(
    const ObBatchExecParamType type,
    const share::ObLSID &ls_id,
    const int64_t merge_version,
    const int64_t batch_size)
  : param_type_(type),
    ls_id_(ls_id),
    compaction_scn_(merge_version),
    batch_size_(batch_size),
    tablet_info_array_()
{
  tablet_info_array_.set_attr(lib::ObMemAttr(MTL_ID(), "BatchArr", ObCtxIds::MERGE_NORMAL_CTX_ID));
}

template<typename ITEM>
int ObBatchExecParam<ITEM>::assign(
    const ObBatchExecParam<ITEM> &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (OB_FAIL(tablet_info_array_.assign(other.tablet_info_array_))) {
    STORAGE_LOG(WARN, "failed to copy tablet ids", KR(ret));
  } else {
    ls_id_ = other.ls_id_;
    compaction_scn_ = other.compaction_scn_;
    batch_size_ = other.batch_size_;
  }
  return ret;
}

template<typename ITEM>
int64_t ObBatchExecParam<ITEM>::get_hash() const
{
  int64_t hash_val = 0;
  hash_val = common::murmurhash(&param_type_, sizeof(param_type_), hash_val);
  hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}

/**
 * -------------------------------------------------------------------ObBatchExecDag-------------------------------------------------------------------
 */
template<typename TASK, typename PARAM>
ObBatchExecDag<TASK, PARAM>::ObBatchExecDag(const share::ObDagType::ObDagTypeEnum type)
  : ObIDag(type),
    collector_(),
    is_inited_(false),
    param_()
{
}

template<typename TASK, typename PARAM>
int ObBatchExecDag<TASK, PARAM>::init_by_param(
    const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const PARAM *init_param = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBatchExecDag has been inited", KR(ret), KPC(this));
  } else if (FALSE_IT(init_param = static_cast<const PARAM *>(param))) {
  } else if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid arguments", KR(ret), KPC(init_param));
  } else if (OB_FAIL(param_.assign(*init_param))) {
    STORAGE_LOG(WARN, "failed to init param", KR(ret), KPC(init_param));
  } else if (OB_FAIL(init_merge_history())) {
    STORAGE_LOG(WARN, "failed to init merge history", KR(ret), KPC(init_param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

template<typename TASK, typename PARAM>
int ObBatchExecDag<TASK, PARAM>::create_first_task()
{
  int ret = OB_SUCCESS;
  TASK *task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBatchExecDag has not inited", KR(ret));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task, 0/*idx*/))) {
    STORAGE_LOG(WARN, "failed to create task", KR(ret));
  }
  return ret;
}

template<typename TASK, typename PARAM>
bool ObBatchExecDag<TASK, PARAM>::operator == (const ObIDag &other) const
{
  bool is_same = true;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else if ((param_.ls_id_ != static_cast<const ObBatchExecDag &>(other).param_.ls_id_)
    || (param_.compaction_scn_ != static_cast<const ObBatchExecDag &>(other).param_.compaction_scn_)) {
    is_same = false;
  }
  return is_same;
}

template<typename TASK, typename PARAM>
int ObBatchExecDag<TASK, PARAM>::fill_info_param(
    ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBatchExecDag not inited", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param,
                                             allocator,
                                             get_type(),
                                             param_.ls_id_.id(),
                                             param_.tablet_info_array_.count()))) {
    STORAGE_LOG(WARN, "failed to fill info param", KR(ret), K(param_));
  }
  return ret;
}

template<typename TASK, typename PARAM>
int ObBatchExecDag<TASK, PARAM>::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_cnt=%ld",
      param_.ls_id_.id(), param_.tablet_info_array_.count()))) {
    STORAGE_LOG(WARN, "failed to fill dag key", K(param_));
  }
  return ret;
}

template<typename TASK, typename PARAM>
int ObBatchExecDag<TASK, PARAM>::init_merge_history()
{
  int ret = OB_SUCCESS;
  ObMergeStaticInfo &static_history = collector_.merge_history_.static_info_;
  static_history.ls_id_ = param_.ls_id_;
  static_history.tablet_id_ = ObTabletID(ObTabletID::INVALID_TABLET_ID); // mock a special tablet id
  static_history.compaction_scn_ = param_.compaction_scn_;
  static_history.merge_type_ = BATCH_EXEC;
  static_history.concurrent_cnt_ = param_.get_task_cnt();
  static_history.exec_mode_ = EXEC_MODE_LOCAL;
  collector_.remain_task_cnt_ = static_history.concurrent_cnt_;
  STORAGE_LOG(INFO, "success to init merge history", KR(ret), K(static_history), K_(param));
  return ret;
}

/**
 * -------------------------------------------------------------------ObBatchExecTask-------------------------------------------------------------------
 */
template<typename TASK, typename PARAM>
int ObBatchExecTask<TASK, PARAM>::init(const int64_t input_idx)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBatchExecTask init twice", KR(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get unexpected null dag", KR(ret));
  } else if (OB_UNLIKELY(!is_batch_exec_dag(dag_->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get unexpected dag type", KR(ret), K(dag_->get_type()));
  } else {
    base_dag_ = static_cast<ObBatchExecDag<TASK, PARAM> *>(dag_);
    if (OB_UNLIKELY(!base_dag_->get_param().is_valid()
        || input_idx * get_batch_size() > base_dag_->get_param().tablet_info_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected not valid param or idx", KR(ret), K(base_dag_->get_param()), K(input_idx));
    } else {
      idx_ = input_idx;
      is_inited_ = true;
    }
  }
  return ret;
}

template<typename TASK, typename PARAM>
int ObBatchExecTask<TASK, PARAM>::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if ((idx_ + 1) * get_batch_size() >= base_dag_->get_param().tablet_info_array_.count()) {
    ret = OB_ITER_END;
  } else {
    TASK *task = NULL;
    if (OB_FAIL(base_dag_->alloc_task(task))) {
      STORAGE_LOG(WARN, "fail to alloc task", K(ret));
    } else if (OB_FAIL(task->init(idx_ + 1))) {
      STORAGE_LOG(WARN, "fail to init task", K(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

template<typename TASK, typename PARAM>
int ObBatchExecTask<TASK, PARAM>::process()
{
  int ret = inner_process();
  base_dag_->add_collect_cnt(cnt_);
  return ret;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_BATCH_EXEC_DAG_H_
