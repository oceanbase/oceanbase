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

#ifndef OCEABASE_SHARE_HA_MONITOR_STRUCT_
#define OCEABASE_SHARE_HA_MONITOR_STRUCT_

#include "share/transfer/ob_transfer_info.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace share
{
enum class ObStorageHADiagType
{
  ERROR_DIAGNOSE = 0,
  PERF_DIAGNOSE = 1,
  MAX_TYPE
};

enum class ObStorageHADiagModule
{
  TRANSFER_ERROR_DIAGNOSE = 0,
  TRANSFER_PERF_DIAGNOSE = 1,
  MAX_MODULE
};

enum class ObStorageHADiagTaskType
{
  TRANSFER_START = 0,
  TRANSFER_DOING = 1,
  TRANSFER_ABORT = 2,
  TRANSFER_START_IN = 3,
  TRANSFER_START_OUT = 4,
  TRANSFER_FINISH_IN = 5,
  TRANSFER_FINISH_OUT = 6,
  TRANSFER_BACKFILLED = 7,
  MAX_TYPE
};

enum class ObStorageHACostItemName : int
{
  // start stage
  TRANSFER_START_BEGIN = 0,
  LOCK_MEMBER_LIST = 1,
  PRECHECK_LS_REPALY_SCN = 2,
  CHECK_START_STATUS_TRANSFER_TABLETS = 3,
  CHECK_SRC_LS_HAS_ACTIVE_TRANS = 4,
  UPDATE_ALL_TABLET_TO_LS = 5,
  LOCK_TABLET_ON_DEST_LS_FOR_TABLE_LOCK = 6,
  BLOCK_AND_KILL_TX = 7,
  REGISTER_TRANSFER_START_OUT = 8,
  DEST_LS_GET_START_SCN = 9,
  CHECK_SRC_LS_HAS_ACTIVE_TRANS_LATER = 10,
  UNBLOCK_TX = 11,
  WAIT_SRC_LS_REPLAY_TO_START_SCN = 12,
  SRC_LS_GET_TABLET_META = 13,
  REGISTER_TRANSFER_START_IN = 14,
  START_TRANS_COMMIT = 15,
  TRANSFER_START_END = 16,
  // doing stage
  TRANSFER_FINISH_BEGIN = 17,
  UNLOCK_SRC_AND_DEST_LS_MEMBER_LIST = 18,
  CHECK_LS_LOGICAL_TABLE_REPLACED = 19,
  LOCK_LS_MEMBER_LIST_IN_DOING = 20,
  CHECK_LS_LOGICAL_TABLE_REPLACED_LATER = 21,
  REGISTER_TRANSFER_FINISH_IN = 22,
  WAIT_TRANSFER_TABLET_STATUS_NORMAL = 23,
  WAIT_ALL_LS_REPLICA_REPLAY_FINISH_SCN = 24,
  REGISTER_TRANSFER_OUT = 25,
  UNLOCK_TABLET_FOR_LOCK = 26,
  UNLOCK_LS_MEMBER_LIST = 27,
  FINISH_TRANS_COMMIT = 28,
  TRANSFER_FINISH_END = 29,
  // abort stage
  TRANSFER_ABORT_BEGIN = 30,
  UNLOCK_MEMBER_LIST_IN_ABORT = 31,
  ABORT_TRANS_COMMIT = 32,
  TRANSFER_ABORT_END = 33,
  // backfill stage
  TRANSFER_BACKFILLED_TABLE_BEGIN = 34,
  TX_BACKFILL = 35,
  TABLET_REPLACE = 36,
  REPLACE_TIMESTAMP = 37,
  TRANSFER_BACKFILLED_END = 38,
  // start stage log replay
  START_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP = 39,
  START_TRANSFER_OUT_LOG_SCN = 40,
  START_TRANSFER_OUT_TX_ID = 41,
  START_TRANSFER_OUT_REPLAY_COST = 42,
  START_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP = 43,
  START_TRANSFER_IN_LOG_SCN = 44,
  START_TRANSFER_IN_TX_ID = 45,
  START_TRANSFER_IN_REPLAY_COST = 46,
  START_CHECK_LS_EXIST_TS = 47,
  START_CHECK_LS_REPLAY_POINT_TS = 48,
  // doing stage log replay
  FINISH_TRANSFER_OUT_FIRST_REPALY_LOG_TIMESTAMP = 49,
  FINISH_TRANSFER_OUT_LOG_SCN = 50,
  FINISH_TRANSFER_OUT_TX_ID = 51,
  FINISH_TRANSFER_OUT_REPLAY_COST = 52,
  FINISH_TRANSFER_IN_FIRST_REPALY_LOG_TIMESTAMP = 53,
  FINISH_TRANSFER_IN_LOG_SCN = 54,
  FINISH_TRANSFER_IN_TX_ID = 55,
  FINISH_TRANSFER_IN_REPLAY_COST = 56,
  // start stage supplement
  UNLOCK_MEMBER_LIST_IN_START = 57,
  // reply stage supplement
  ON_REGISTER_SUCCESS = 58,
  ON_REPLAY_SUCCESS = 59,
  // backfilled stage supplement
  PREPARE_MERGE_CTX = 60,
  PREPARE_INDEX_TREE = 61,
  DO_BACKFILL_TX = 62,
  TRANSFER_REPLACE_BEGIN = 63,
  TRANSFER_REPLACE_END = 64,
  TRANSFER_BACKFILL_START = 65,
  // start
  STOP_LS_SCHEDULE_MEMDIUM = 66,
  MAX_NAME
};

enum class ObStorageHACostItemType
{
  ACCUM_COST_TYPE = 0,
  FLUENT_TIMESTAMP_TYPE = 1,
  CRUCIAL_TIMESTAMP = 2,
  MAX_TYPE
};

struct ObStorageHAPerfDiagParams final
{
public:
  ObStorageHAPerfDiagParams();
  ~ObStorageHAPerfDiagParams();
  void reset();
  bool is_valid() const;
  int assign(const ObStorageHAPerfDiagParams &params);

  TO_STRING_KV(K_(dest_ls_id), K_(task_id), K_(task_type),
      K_(item_type), K_(name), K_(tablet_id), K_(tablet_count));

  ObLSID dest_ls_id_;
  share::ObTransferTaskID task_id_;
  ObStorageHADiagTaskType task_type_;
  ObStorageHACostItemType item_type_;
  ObStorageHACostItemName name_;
  common::ObTabletID tablet_id_;
  int64_t tablet_count_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAPerfDiagParams);
};

struct ObStorageHADiagTaskKey final
{
public:
  ObStorageHADiagTaskKey();
  ~ObStorageHADiagTaskKey() { reset(); }
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObStorageHADiagTaskKey &key) const;
  bool operator<(const ObStorageHADiagTaskKey &key) const;
  int assign(const ObStorageHADiagTaskKey &key);

  TO_STRING_KV(
    K_(tenant_id),
    K_(task_id),
    K_(module),
    K_(type),
    K_(retry_id),
    K_(diag_type),
    K_(tablet_id));

  uint64_t tenant_id_;
  int64_t task_id_;
  ObStorageHADiagModule module_;
  ObStorageHADiagTaskType type_;
  int64_t retry_id_;
  ObStorageHADiagType diag_type_;
  common::ObTabletID tablet_id_;
};

struct ObStorageHADiagTask;
struct ObStorageHADiagInfo
{
public:
  ObStorageHADiagInfo();
  virtual ~ObStorageHADiagInfo() { reset(); }
  virtual int get_info(char *info, const int64_t size) const = 0;
  virtual int update(const ObStorageHADiagInfo &info) = 0;
  virtual int assign(const ObStorageHADiagInfo &other);
  virtual int deep_copy(ObIAllocator &allocator, ObStorageHADiagInfo *&out_info) const = 0;
  virtual int get_task_id(char *info, const int64_t size) const = 0;
  const char * get_module_str() const;
  const char * get_type_str() const;
  const char * get_transfer_error_diagnose_msg() const;
  void reset();
  bool is_valid() const;

  const static char *ObStorageDiagModuleStr[static_cast<int64_t>(ObStorageHADiagModule::MAX_MODULE)];
  const static char *ObStorageDiagTaskTypeStr[static_cast<int64_t>(ObStorageHADiagTaskType::MAX_TYPE)];
  const static char *ObTransferErrorDiagMsg[static_cast<int>(share::ObStorageHACostItemName::MAX_NAME)];

  TO_STRING_KV(
    K_(task_id),
    K_(ls_id),
    K_(module),
    K_(type),
    K_(retry_id),
    K_(result_code),
    K_(timestamp),
    K_(result_msg),
    K_(tablet_id));

  int64_t task_id_;
  ObLSID ls_id_;
  ObStorageHADiagModule module_;
  ObStorageHADiagTaskType type_;
  int64_t retry_id_;
  int64_t result_code_;
  int64_t timestamp_;
  ObStorageHACostItemName result_msg_;
  ObStorageHADiagTask *task_; // TODO(zhixing.yh) exist risk，it will be deleted after key placed in info
  ObTabletID tablet_id_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagInfo);
};

struct ObStorageHADiagTask : public common::ObDLinkBase<ObStorageHADiagTask>
{
public:
  ObStorageHADiagTask();
  virtual ~ObStorageHADiagTask();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(key), KPC_(val));

  ObStorageHADiagTaskKey key_;
  ObStorageHADiagInfo *val_;
};

struct ObTransferErrorDiagInfo : public ObStorageHADiagInfo
{
public:
  ObTransferErrorDiagInfo();
  virtual ~ObTransferErrorDiagInfo() { reset(); }
  virtual int get_info(char *info, const int64_t size) const override;
  virtual int get_task_id(char *info, const int64_t size) const override;
  virtual int update(const ObStorageHADiagInfo &info) override;
  virtual int deep_copy(ObIAllocator &allocator, ObStorageHADiagInfo *&out_info) const override;
  virtual int assign(const ObStorageHADiagInfo &another) override;
  bool is_valid() const;
  void reset();

  INHERIT_TO_STRING_KV(
    "ObStorageHADiagInfo",
    ObStorageHADiagInfo,
    K_(trace_id),
    K_(thread_id));

  common::ObCurTraceId::TraceId trace_id_;
  int64_t thread_id_;
};

struct ObIStorageHACostItem : public common::ObDLinkBase<ObIStorageHACostItem>
{
public:
  ObIStorageHACostItem();
  virtual ~ObIStorageHACostItem() { reset(); }
  virtual int update(const ObIStorageHACostItem &item) = 0;
  virtual int get_str(char *str, const int64_t size, int64_t &pos) const = 0;
  virtual int assign(const ObIStorageHACostItem &item);
  virtual int deep_copy(ObIAllocator &allocator, ObIStorageHACostItem *&item) const = 0;
  virtual uint32_t get_size() const;
  bool is_valid() const;
  void reset();
  const char * get_item_name() const;
  const char * get_item_type() const;

  const static char *ObStorageHACostItemNameStr[static_cast<int64_t>(ObStorageHACostItemName::MAX_NAME)];
  const static char *ObStorageHACostItemTypeStr[static_cast<int64_t>(ObStorageHACostItemType::MAX_TYPE)];

  TO_STRING_KV(
    K_(name),
    K_(type),
    K_(retry_id));

public:
  ObStorageHACostItemName name_;
  ObStorageHACostItemType type_;
  int64_t retry_id_;
};

struct ObStorageHACostAccumItem : public ObIStorageHACostItem
{
public:
  ObStorageHACostAccumItem();
  virtual ~ObStorageHACostAccumItem() { reset(); }
  virtual int update(const ObIStorageHACostItem &item) override;
  virtual int get_str(char *str, const int64_t size, int64_t &pos) const override;
  virtual int deep_copy(ObIAllocator &allocator, ObIStorageHACostItem *&item) const override;
  virtual uint32_t get_size() const override;
  bool is_valid() const;
  void reset();
  virtual int assign(const ObIStorageHACostItem &item) override;

  INHERIT_TO_STRING_KV(
    "ObIStorageHACostItem",
    ObIStorageHACostItem,
    K_(cost_time));
public:
  uint64_t cost_time_;
};

struct ObStorageHATimestampItem : public ObIStorageHACostItem
{
public:
  ObStorageHATimestampItem();
  virtual ~ObStorageHATimestampItem() { reset(); }
  virtual int update(const ObIStorageHACostItem &item) override;
  virtual int get_str(char *str, const int64_t size, int64_t &pos) const override;
  virtual int assign(const ObIStorageHACostItem &item) override;
  virtual int deep_copy(ObIAllocator &allocator, ObIStorageHACostItem *&item) const override;
  virtual uint32_t get_size() const override;
  bool is_valid() const;
  void reset();

  INHERIT_TO_STRING_KV(
    "ObIStorageHACostItem",
    ObIStorageHACostItem,
    K_(timestamp));
public:
  uint64_t timestamp_;
};

struct ObTransferPerfDiagInfo : public ObStorageHADiagInfo
{
public:
  ObTransferPerfDiagInfo();
  virtual ~ObTransferPerfDiagInfo();
  typedef ObDList<ObIStorageHACostItem> ItemList;
  typedef hash::ObHashMap<int, ObIStorageHACostItem *> ItemMap;
  int init(ObIAllocator *allocator, const int64_t tenant_id);
  virtual int update(const ObStorageHADiagInfo &info) override;
  virtual int deep_copy(ObIAllocator &allocator, ObStorageHADiagInfo *&out_info) const override;
  virtual int get_task_id(char *info, const int64_t size) const override;
  virtual int get_info(char *info, const int64_t size) const override;
  virtual int assign(const ObStorageHADiagInfo &info) override;
  int copy_item_list(const ObTransferPerfDiagInfo &info);
  int add_item(const ObIStorageHACostItem &item);
  bool is_inited() const;
  bool is_valid() const;
  void reuse();
  void clear();
  void destroy();

  INHERIT_TO_STRING_KV(
    "ObStorageHADiagInfo",
    ObStorageHADiagInfo,
    K_(is_inited),
    K_(end_timestamp),
    K_(tablet_count),
    K_(items));

private:
  int find_item_(const ObStorageHACostItemName &name, bool &found, ObIStorageHACostItem *&item);
  int update_item_list_(const ObTransferPerfDiagInfo &info);
  static constexpr int64_t PERF_DIAG_BUCKET_NUM = 100;
  static constexpr int64_t ITEM_MAX_SIZE = (1 << 12); // 4K

public:
  uint64_t end_timestamp_;
  int64_t tablet_count_;
  ItemList items_;
  ItemMap item_map_;
  uint64_t item_size_;

private:
  bool is_inited_;
  ObIAllocator *allocator_;
};

}
}
#endif