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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_TASK_
#define OCEANBASE_STORAGE_DDL_MERGE_TASK_

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"

namespace oceanbase
{

namespace share
{
struct ObDDLChecksumItem;
}

namespace storage
{
class ObLS;

struct ObDDLTableMergeDagParam : public share::ObIDagInitParam
{
public:
  ObDDLTableMergeDagParam()
    : ls_id_(),
      tablet_id_(),
      rec_scn_(share::SCN::min_scn()),
      is_commit_(false),
      start_scn_(share::SCN::min_scn()),
      table_id_(0),
      execution_id_(-1),
      ddl_task_id_(0)
  { }
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid() && start_scn_.is_valid_and_not_min();
  }
  virtual ~ObDDLTableMergeDagParam() = default;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(rec_scn), K_(is_commit), K_(start_scn),
    K_(table_id), K_(execution_id), K_(ddl_task_id));
public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN rec_scn_;
  bool is_commit_;
  share::SCN start_scn_; // start log ts at schedule, for skipping expired task
  uint64_t table_id_; // used for report ddl checksum
  int64_t execution_id_; // used for report ddl checksum
  int64_t ddl_task_id_; // used for report ddl checksum
};

class ObDDLTableMergeDag : public share::ObIDag
{
public:
  ObDDLTableMergeDag();
  virtual ~ObDDLTableMergeDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(ddl_param), K_(compat_mode));
public:
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool ignore_warning() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return compat_mode_; }
private:
  bool is_inited_;
  ObDDLTableMergeDagParam ddl_param_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLTableMergeDag);
};

class ObDDLMacroBlock;
class ObDDLKV;

// each task process only one ddl kv
class ObDDLTableDumpTask : public share::ObITask
{
public:
  ObDDLTableDumpTask();
  virtual ~ObDDLTableDumpTask();
  int init(const share::ObLSID &ls_id, const ObTabletID &tablet_id, const share::SCN &freeze_scn);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id), K_(freeze_scn));
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN freeze_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLTableDumpTask);
};

class ObDDLTableMergeTask : public share::ObITask
{
public:
  ObDDLTableMergeTask();
  virtual ~ObDDLTableMergeTask();
  int init(const ObDDLTableMergeDagParam &ddl_dag_param);
  virtual int process() override;
  TO_STRING_KV(K_(is_inited), K_(merge_param));
private:
  bool is_inited_;
  ObDDLTableMergeDagParam merge_param_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLTableMergeTask);
};

struct ObTabletDDLParam final
{
public:
  ObTabletDDLParam();
  ~ObTabletDDLParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(table_key), K_(start_scn), K_(commit_scn), K_(snapshot_version), K_(cluster_version));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObITable::TableKey table_key_;
  share::SCN start_scn_;
  share::SCN commit_scn_;
  int64_t snapshot_version_;
  int64_t cluster_version_;
};

class ObTabletDDLUtil
{
public:
  static int prepare_index_data_desc(const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id,
                                     const int64_t snapshot_version,
                                     const int64_t cluster_version,
                                     const blocksstable::ObSSTable *first_ddl_sstable,
                                     blocksstable::ObDataStoreDesc &data_desc);

  static int create_ddl_sstable(const ObTabletDDLParam &ddl_param,
                                const ObIArray<const blocksstable::ObDataMacroBlockMeta *> &meta_array,
                                const blocksstable::ObSSTable *first_ddl_sstable,
                                ObTableHandleV2 &table_handle);

  static int create_ddl_sstable(blocksstable::ObSSTableIndexBuilder *sstable_index_builder,
                                const ObTabletDDLParam &ddl_param,
                                const blocksstable::ObSSTable *first_ddl_sstable,
                                ObTableHandleV2 &table_handle);

  static int update_ddl_table_store(const ObTabletDDLParam &ddl_param,
                                    const ObTableHandleV2 &table_handle);

  static int compact_ddl_sstable(const ObIArray<ObITable *> &ddl_sstables,
                                 const ObTableReadInfo &read_info,
                                 const ObTabletDDLParam &ddl_param,
                                 ObTableHandleV2 &table_handle);

  static int report_ddl_checksum(const share::ObLSID &ls_id,
                                 const ObTabletID &tablet_id,
                                 const uint64_t table_id,
                                 const int64_t execution_id,
                                 const int64_t ddl_task_id,
                                 const ObIArray<int64_t> &column_checksums);
  static int check_and_get_major_sstable(const share::ObLSID &ls_id,
                                         const ObTabletID &tablet_id,
                                         const blocksstable::ObSSTable *&latest_major_sstable);

  static int check_data_integrity(const ObTablesHandleArray &ddl_sstables,
                                  const share::SCN &start_scn,
                                  const share::SCN &prepare_scn,
                                  bool &is_data_complete);
};

} // namespace storage
} // namespace oceanbase

#endif
