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

#ifndef STORAGE_COMPACTION_OB_TABLET_MERGE_TASK_H_
#define STORAGE_COMPACTION_OB_TABLET_MERGE_TASK_H_

#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_i_table.h"
#include "observer/report/ob_i_meta_report.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/compaction/ob_i_compaction_filter.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{
namespace storage
{
class ObITable;
class ObTablet;
class ObTabletHandle;
struct ObUpdateTableStoreParam;
}

namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{
using namespace storage;
class ObBasicTabletMergeDag;
class ObTabletMergeDag;
struct ObTabletMergeCtx;
class ObTabletMergeInfo;
class ObPartitionMerger;
struct ObCachedTransStateMgr;


struct ObMergeParameter {
  ObMergeParameter();
  ~ObMergeParameter() { reset(); }
  bool is_valid() const;
  void reset();
  int init(ObTabletMergeCtx &merge_ctx, const int64_t idx);

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;

  storage::ObLSHandle ls_handle_;
  storage::ObTablesHandleArray *tables_handle_;
  ObMergeType merge_type_;
  ObMergeLevel merge_level_;
  const ObStorageSchema *merge_schema_;
  blocksstable::ObDatumRange merge_range_;
  int16_t sstable_logic_seq_;
  ObVersionRange version_range_;
  share::ObScnRange scn_range_;
  const ObITableReadInfo *rowkey_read_info_;
  bool is_full_merge_;               // full merge or increment merge, duplicated with merge_level
  compaction::ObCachedTransStateMgr *trans_state_mgr_;
  share::SCN merge_scn_;
  TO_STRING_KV(KPC_(tables_handle), K_(merge_type), K_(merge_level), KP_(merge_schema),
               K_(merge_range), K_(version_range), K_(scn_range), K_(is_full_merge), K_(merge_scn));
private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeParameter);
};

struct ObTabletMergeDagParam : public share::ObIDagInitParam
{
  ObTabletMergeDagParam();
  ObTabletMergeDagParam(
    const storage::ObMergeType merge_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id);
  virtual bool is_valid() const override;
  storage::ObMergeType get_merge_type() const
  {
    return is_tenant_major_merge_ ? MAJOR_MERGE : merge_type_;
  }

  virtual int64_t to_string(char* buf, const int64_t buf_len) const;

  bool for_diagnose_;
  bool is_tenant_major_merge_;
  bool need_swap_tablet_flag_;
  storage::ObMergeType merge_type_;
  int64_t merge_version_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  observer::ObIMetaReport *report_;
};

class ObTabletMergePrepareTask: public share::ObITask
{
public:
  ObTabletMergePrepareTask();
  virtual ~ObTabletMergePrepareTask();
  int init();
protected:
  virtual int process() override;
private:
  int build_merge_ctx(bool &skip_merge_task_flag);
  virtual int check_before_init() { return OB_SUCCESS; }
  virtual int inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag) = 0;

protected:
  bool is_inited_;
  ObBasicTabletMergeDag *merge_dag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergePrepareTask);
};

class ObTabletMajorPrepareTask: public ObTabletMergePrepareTask
{
public:
  ObTabletMajorPrepareTask() {}
  virtual ~ObTabletMajorPrepareTask() {}
private:
  virtual int check_before_init() override;
  virtual int inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMajorPrepareTask);
};

class ObTabletMiniPrepareTask: public ObTabletMergePrepareTask
{
public:
  ObTabletMiniPrepareTask() {}
  virtual ~ObTabletMiniPrepareTask() {}
private:
  virtual int inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMiniPrepareTask);
};

class ObTabletMergeFinishTask: public share::ObITask
{
public:
  ObTabletMergeFinishTask();
  virtual ~ObTabletMergeFinishTask();
  int init();
  virtual int process() override;

private:
  int create_sstable_after_merge();
  int get_merged_sstable(ObTabletMergeCtx &ctx);
  int add_sstable_for_merge(ObTabletMergeCtx &ctx);
  int try_schedule_compaction_after_mini(ObTabletMergeCtx &ctx, storage::ObTabletHandle &tablet_handle);
  int try_report_tablet_stat_after_mini(ObTabletMergeCtx &ctx);
private:
  bool is_inited_;
  ObBasicTabletMergeDag *merge_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeFinishTask);
};

class ObMergeDagHash
{
public:
  ObMergeDagHash()
   : merge_type_(storage::ObMergeType::INVALID_MERGE_TYPE),
     ls_id_(),
     tablet_id_()
  {}
  virtual ~ObMergeDagHash() {}

  virtual int64_t inner_hash() const;
  bool belong_to_same_tablet(const ObMergeDagHash *other) const;

  TO_STRING_KV(K_(merge_type), K_(ls_id), K_(tablet_id));

  ObMergeType merge_type_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
};

class ObBasicTabletMergeDag: public share::ObIDag, public ObMergeDagHash
{
public:
  ObBasicTabletMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObBasicTabletMergeDag();
  ObTabletMergeCtx *get_ctx() { return ctx_; }
  ObTabletMergeDagParam &get_param() { return param_; }
  virtual const share::ObLSID & get_ls_id() const { return param_.ls_id_; }
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool ignore_warning() override
  {
    return OB_NO_NEED_MERGE == dag_ret_
        || OB_TABLE_IS_DELETED == dag_ret_
        || OB_TENANT_HAS_BEEN_DROPPED == dag_ret_
        || OB_LS_NOT_EXIST == dag_ret_
        || OB_TABLET_NOT_EXIST == dag_ret_
        || OB_CANCELED == dag_ret_;
  }
  int prepare_merge_ctx();
  int get_tablet_and_compat_mode();
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  static int generate_merge_task(
      ObBasicTabletMergeDag &merge_dag,
      ObTabletMergeCtx &ctx,
      share::ObITask *prepare_task = nullptr);
protected:
  int alloc_merge_ctx();
  int inner_init(const ObTabletMergeDagParam &param);

  bool is_inited_;
  lib::Worker::CompatMode compat_mode_;
  ObTabletMergeCtx *ctx_;
  ObTabletMergeDagParam param_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObBasicTabletMergeDag);
};

class ObTabletMergeDag : public ObBasicTabletMergeDag
{
public:
  ObTabletMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObTabletMergeDag() {}
  template <typename T>
  int create_first_task();

  virtual int gene_compaction_info(compaction::ObTabletCompactionProgress &progress) override;
  virtual int diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress) override;
};

template <typename T>
int ObTabletMergeDag::create_first_task()
{
  int ret = common::OB_SUCCESS;
  T *task = nullptr;
  if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret), K_(ls_id), K_(tablet_id), K_(ctx));
  }
  return ret;
}

class ObTabletMajorMergeDag: public ObTabletMergeDag
{
public:
  ObTabletMajorMergeDag();
  virtual ~ObTabletMajorMergeDag();
  virtual int create_first_task() override
  {
    return ObTabletMergeDag::create_first_task<ObTabletMajorPrepareTask>();
  }
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMajorMergeDag);
};

class ObTabletMiniMergeDag: public ObTabletMergeDag
{
public:
  ObTabletMiniMergeDag();
  virtual ~ObTabletMiniMergeDag();
  virtual int create_first_task() override
  {
    return ObTabletMergeDag::create_first_task<ObTabletMiniPrepareTask>();
  }
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMiniMergeDag);
};

class ObTabletMergeExecutePrepareTask: public share::ObITask
{
public:
  ObTabletMergeExecutePrepareTask();
  virtual ~ObTabletMergeExecutePrepareTask();
  int init(const ObGetMergeTablesResult &result, ObTabletMergeCtx &ctx, const bool need_swap_tablet_flag);
  virtual int process() override;
protected:
  virtual int prepare_compaction_filter() { return OB_SUCCESS; }
  int get_tablet_and_result();
  int get_result_by_table_key();
  const static int64_t TABLET_KEY_ARRAY_CNT = 20;

  bool is_inited_;
  bool need_swap_tablet_flag_;
  ObTabletMergeCtx *ctx_;
  ObGetMergeTablesResult result_;
  ObSEArray<ObITable::TableKey, TABLET_KEY_ARRAY_CNT> table_key_array_;
};

// for minor merge
class ObTxTableMergeExecutePrepareTask : public ObTabletMergeExecutePrepareTask
{
protected:
  virtual int prepare_compaction_filter() override;
};

class ObTabletMergeExecuteDag: public ObTabletMergeDag
{
public:
  ObTabletMergeExecuteDag();
  virtual ~ObTabletMergeExecuteDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override; // for diagnose
  int direct_init_ctx(
      const ObTabletMergeDagParam &param,
      const lib::Worker::CompatMode compat_mode,
      const ObGetMergeTablesResult &result,
      storage::ObLSHandle &ls_handle);
  template<class T>
  int create_first_task(const ObGetMergeTablesResult &result, const bool need_swap_tablet_flag);
  virtual bool operator == (const ObIDag &other) const override;
  const share::ObScnRange& get_merge_range() const { return merge_scn_range_; }

  INHERIT_TO_STRING_KV("ObBasicTabletMergeDag", ObBasicTabletMergeDag, K_(merge_scn_range));
private:
  virtual int create_first_task(const ObGetMergeTablesResult &result, const bool need_swap_tablet_flag);
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeExecuteDag);

  share::ObScnRange merge_scn_range_;
};

class ObTxTableMinorExecuteDag: public ObTabletMergeExecuteDag
{
public:
  ObTxTableMinorExecuteDag()
    : compaction_filter_()
  {}
  virtual ~ObTxTableMinorExecuteDag() = default;
private:
  virtual int create_first_task(const ObGetMergeTablesResult &result, const bool need_swap_tablet_flag) override;
  DISALLOW_COPY_AND_ASSIGN(ObTxTableMinorExecuteDag);
  ObTransStatusFilter compaction_filter_;
};

class ObTabletMergeTask: public share::ObITask
{
public:
  ObTabletMergeTask();
  virtual ~ObTabletMergeTask();
  int init(const int64_t idx, ObTabletMergeCtx &ctx);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
private:
  common::ObArenaAllocator allocator_;
  int64_t idx_;
  ObTabletMergeCtx *ctx_;
  ObPartitionMerger *merger_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeTask);
};

} // namespace compaction
} // namespace oceanbase

#endif
