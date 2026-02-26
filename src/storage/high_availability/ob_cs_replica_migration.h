/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEABASE_STORAGE_CS_REPLICA_MIGRATION_
#define OCEABASE_STORAGE_CS_REPLICA_MIGRATION_

#include "storage/high_availability/ob_ls_migration.h"

namespace oceanbase
{
namespace storage
{

struct ObTabletCOConvertCtx
{
public:
  enum class Status
  {
    UNKNOWN         = 0,  // intial state, need take tablet_id and storage schema into consideration
    PROGRESSING     = 1,  // need convert and to be check result
    FINISHED        = 2,  // finish convert, or tablet is deleted
    RETRY_EXHAUSTED = 3,  // retry times >= MAX_RETRY_CNT
    MAX_STATUS
  };
public:
  ObTabletCOConvertCtx();
  virtual ~ObTabletCOConvertCtx();
  int init(const ObTabletID &tablet_id, const share::ObDagId &co_dag_net_id);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(co_dag_net_id), K_(status), K_(retry_cnt), K_(is_inited));
public:
  OB_INLINE bool is_unknown() const { return Status::UNKNOWN == status_; }
  OB_INLINE bool is_progressing() const { return Status::PROGRESSING == status_; }
  OB_INLINE bool is_finished() const { return Status::FINISHED == status_; }
  OB_INLINE bool is_retry_exhausted() const { return retry_cnt_ >= MAX_RETRY_CNT; }
  OB_INLINE bool is_eagain_exhausted() const { return eagain_cnt_ >= MAX_EAGAIN_CNT; }
  void set_progressing();
  OB_INLINE void set_finished() { status_ = Status::FINISHED; }
  OB_INLINE void set_retry_exhausted() { status_ = Status::RETRY_EXHAUSTED; }
  OB_INLINE void inc_retry_cnt() { retry_cnt_++; }
  OB_INLINE void inc_eagain_cnt() { eagain_cnt_++; }
public:
  const static int64_t MAX_RETRY_CNT = 3;
  const static int64_t MAX_EAGAIN_CNT = 9; // allow at most 9 * OB_DATA_TABLETS_NOT_CHECK_CONVERT_THRESHOLD (20min) = 3h
public:
  ObTabletID tablet_id_;
  share::ObDagId co_dag_net_id_;
  Status status_;
  int64_t retry_cnt_;
  int64_t eagain_cnt_;
  bool is_inited_;
};

class ObHATabletGroupCOConvertCtx : public ObHATabletGroupCtx
{
public:
  ObHATabletGroupCOConvertCtx();
  virtual ~ObHATabletGroupCOConvertCtx();
public:
  virtual void reuse() override;
  virtual void inner_reuse() override;
  virtual int inner_init() override;
  void inc_finish_migration_cnt();
  bool ready_to_check() const;
  bool is_all_state_deterministic() const;
  int set_convert_status(const ObTabletID &tablet_id, const ObTabletCOConvertCtx::Status status);
  OB_INLINE int set_convert_finsih(const ObTabletID &tablet_id) { return set_convert_status(tablet_id, ObTabletCOConvertCtx::Status::FINISHED); }
  OB_INLINE int set_convert_progressing(const ObTabletID &tablet_id) { return set_convert_status(tablet_id, ObTabletCOConvertCtx::Status::PROGRESSING); }
  int get_co_dag_net_id(const ObTabletID &tablet_id, share::ObDagId &co_dag_net_id) const;
  int check_and_schedule(ObLS &ls);
  // move tablet_id_array last to prevent log ignore other parameters
  TO_STRING_KV(K_(finish_migration_cnt), K_(finish_check_cnt), K_(retry_exhausted_cnt), "map_size", idx_map_.size(), K_(convert_ctxs), K_(index), K_(tablet_id_array));
public:
  static int check_need_convert(const ObTablet &tablet, bool &need_convert);
  static int update_deleted_data_tablet_status(
      ObHATabletGroupCtx *tablet_group_ctx,
      const ObTabletID &tablet_id);
private:
  void inner_set_convert_finish(ObTabletCOConvertCtx &convert_ctx);
  void inner_set_retry_exhausted(ObTabletCOConvertCtx &convert_ctx);
  int inner_get_valid_convert_ctx_idx(const ObTabletID &tablet_id, int64_t &idx) const;
  int inner_check_and_schedule(ObLS &ls, const ObTabletID &tablet_id);
private:
  // refer to TransferTableMap
  const static int64_t TABLET_CONVERT_CTX_MAP_BUCKED_NUM = 128; // a tablet group contains 2G size of tablets, 1024 macro block
  typedef common::hash::ObHashMap<ObTabletID, int64_t> TabletConvertCtxIndexMap;
public:
  int64_t finish_migration_cnt_;
  int64_t finish_check_cnt_;
  int64_t retry_exhausted_cnt_;
  TabletConvertCtxIndexMap idx_map_;
  ObArray<ObTabletCOConvertCtx> convert_ctxs_;
  DISALLOW_COPY_AND_ASSIGN(ObHATabletGroupCOConvertCtx);
};

class ObDataTabletsCheckCOConvertDag : public ObMigrationDag
{
public:
  enum class ObCheckScheduleReason {
    MIGRATION_FAILED  = 0,
    READY_TO_CHECK    = 1,
    ALL_DETERMINISTIC = 2,
    WAIT_TIME_EXCEED  = 3,
    CONVERT_DISABLED  = 4,
    MAX_NOT_SCHEDULE,
  };
public:
  ObDataTabletsCheckCOConvertDag();
  virtual ~ObDataTabletsCheckCOConvertDag();
  virtual bool check_can_schedule() override;
  virtual int create_first_task() override;
  virtual int report_result() override;
  int init(
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObLS *ls);
  int check_convert_ctx_valid(ObIHADagNetCtx *ha_dag_net_ctx);
public:
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  INHERIT_TO_STRING_KV("ObIMigrationDag", ObMigrationDag, KP(this), KPC_(ls), K_(first_start_time), K_(is_inited));
private:
  int inner_check_can_schedule(ObMigrationCtx &migration_ctx, bool &can_schedule);
public:
#ifdef ERRSIM
  const static int64_t OB_DATA_TABLETS_NOT_CHECK_CONVERT_THRESHOLD = 30 * 1000 * 1000; /*30s*/
#else
  const static int64_t OB_DATA_TABLETS_NOT_CHECK_CONVERT_THRESHOLD = 20 * 60 * 1000 * 1000; /*20min*/
#endif
private:
  ObLS *ls_;
  int64_t first_start_time_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsCheckCOConvertDag);
} ;

class ObDataTabletsCheckConvertTask : public ObITask
{
public:
  ObDataTabletsCheckConvertTask();
  virtual ~ObDataTabletsCheckConvertTask();
  int init(
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObLS *ls);
private:
  virtual int process() override;
private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  ObLS *ls_;
  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsCheckConvertTask);
};

} // namespace storage
} // namespace oceanbase

#endif