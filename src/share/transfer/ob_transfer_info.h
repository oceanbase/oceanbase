/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCENABASE_SHARE_OB_TRANSFER_INFO_H
#define OCENABASE_SHARE_OB_TRANSFER_INFO_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h" // ObLSID
#include "share/ob_define.h"
#include "share/ob_balance_define.h"   // ObBalanceTaskID, ObTransferTaskID
#include "share/schema/ob_schema_struct.h" // ObTableType, ObIndexType
#include "share/ob_display_list.h"   // ObDisplayList
#include "common/ob_tablet_id.h" // ObTabletID
#include "share/scn.h" // SCN
#include "storage/tablelock/ob_table_lock_common.h" // ObTableLockOwnerID

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{
class ObLockAloneTabletRequest;
}
}

namespace share
{
namespace schema
{
  class ObBasePartition;
}

static const int64_t OB_INVALID_TRANSFER_SEQ = -1;

/////////////// ObTransferStatus ///////////////
struct ObTransferStatus final
{
public:
  enum STATUS : uint8_t
  {
    INIT = 0,
    START = 1,
    DOING = 2,
    COMPLETED = 3,
    FAILED = 4,
    ABORTED = 5,
    CANCELED = 6,
    MAX_STATUS
  };
public:
  ObTransferStatus() : status_(MAX_STATUS) {}
  ~ObTransferStatus() = default;
  explicit ObTransferStatus(const STATUS &status) : status_(status) {}

  STATUS status() const { return status_; }
  operator STATUS() const { return status_; }
  bool is_valid() const { return INIT <= status_ && status_ < MAX_STATUS; }
  void reset() { status_ = MAX_STATUS; }
  ObTransferStatus &operator=(const ObTransferStatus &status);
  ObTransferStatus &operator=(const STATUS &status);
  const char *str() const;
  int parse_from_str(const ObString &str);

  bool is_init_status() const { return INIT == status_; }
  bool is_start_status() const { return START == status_; }
  bool is_doing_status() const { return DOING == status_; }
  bool is_aborted_status() const { return ABORTED == status_; }
  bool is_canceled_status() const { return CANCELED == status_; }
  bool is_failed_status() const { return FAILED == status_; }
  bool is_completed_status() const { return COMPLETED == status_; }
  // CANCELED/FAILED/COMPLETED
  bool is_finish_status() const;
  bool operator ==(const ObTransferStatus &other) const { return status_ == other.status_; }
  bool operator !=(const ObTransferStatus &other) const { return status_ != other.status_; }

  TO_STRING_KV(K_(status), "status", str());
private:
  STATUS status_;
};

struct ObTransferStatusHelper
{
  static int check_can_change_status(
      const ObTransferStatus &old_status,
      const ObTransferStatus &new_status,
      bool &can_change);
};

/////////////// ObTransferRefreshStatus///////////////
// For auto refresh tablet location
class ObTransferRefreshStatus final
{
public:
 enum STATUS : uint8_t
 {
   UNKNOWN = 0,
   DOING = 1,
   DONE = 2,
   INVALID
 };
 ObTransferRefreshStatus() : status_(INVALID) {}
 ~ObTransferRefreshStatus() {}
 explicit ObTransferRefreshStatus(const ObTransferRefreshStatus &other) : status_(other.status_) {}
 explicit ObTransferRefreshStatus(const ObTransferRefreshStatus::STATUS &status) : status_(status) {}

 ObTransferRefreshStatus &operator=(const ObTransferRefreshStatus &other);
 ObTransferRefreshStatus &operator=(const ObTransferRefreshStatus::STATUS &status);

 bool is_valid() const { return UNKNOWN <= status_ && status_ < INVALID; }
 void reset() { status_ = INVALID; }
 const char *str() const;

 bool is_unknown_status() const { return UNKNOWN == status_; }
 bool is_doing_status() const { return DOING == status_; }
 bool is_done_status() const { return DONE == status_; }

 void convert_from(const ObTransferStatus &status);
 void update(const ObTransferRefreshStatus &other, bool &changed);

 TO_STRING_KV(K_(status), "status", str());
private:
  STATUS status_;
};

class ObTransferRefreshInfo final
{
public:
  ObTransferRefreshInfo() : task_id_(), status_() {}
  ~ObTransferRefreshInfo() {}

  int init(const ObTransferTaskID &task_id,
           const ObTransferRefreshStatus &status);
  void reset();

  const ObTransferTaskID &get_task_id() const { return task_id_; }
  ObTransferRefreshStatus &get_status() { return status_; }
  const ObTransferRefreshStatus &get_status() const { return status_; }

  static bool less_than(const ObTransferRefreshInfo &left, const ObTransferRefreshInfo &right)
  { return left.task_id_ < right.task_id_; }

  TO_STRING_KV(K_(task_id), K_(status));
private:
  ObTransferTaskID task_id_;
  ObTransferRefreshStatus status_;
};

/////////////// ObTransferTabletInfo ///////////////
// <TabletID:TransferSeq>
// Represents a Tablet for Transfer
struct ObTransferTabletInfo final : public ObDisplayType
{
  OB_UNIS_VERSION(1);
public:
  ObTransferTabletInfo();
  ~ObTransferTabletInfo() = default;
  void reset();
  bool is_valid() const { return tablet_id_.is_valid() && transfer_seq_ > OB_INVALID_TRANSFER_SEQ; }
  const ObTabletID &tablet_id() const { return tablet_id_; }
  int64_t transfer_seq() const { return transfer_seq_; }

  // define init to avoid initializing variables separately
  int init(const ObTabletID &tablet_id, const int64_t transfer_seq);
  bool operator==(const ObTransferTabletInfo &other) const;

  ///////////////////////// display string related ////////////////////////
  // "table_id:part_id" max length: 20 + 20 + ':' + '\0'
  int64_t max_display_str_len() const { return 42; }
  // parse from string "tablet_id:transfer_seq"
  int parse_from_display_str(const common::ObString &str);
  // generate string "tablet_id:transfer_seq"
  int to_display_str(char *buf, const int64_t len, int64_t &pos) const;

  TO_STRING_KV(K_(tablet_id), K_(transfer_seq));
public:
  ObTabletID tablet_id_;
  int64_t transfer_seq_;
};

typedef ObDisplayList<share::ObTransferTabletInfo> ObTransferTabletList;

/////////////// ObTransferPartInfo ///////////////
// <table_id:part_object_id>
// Represents a partition participating in the Transfer
struct ObTransferPartInfo final : public ObDisplayType
{
  struct Compare final
  {
  public:
    Compare() {}
    ~Compare() {}
    bool operator() (const ObTransferPartInfo &left, const ObTransferPartInfo &right)
    {
      return (left.table_id() == right.table_id())
          ? (left.part_object_id() < right.part_object_id())
          : (left.table_id() < right.table_id());
    }
  };

  ObTransferPartInfo() : table_id_(OB_INVALID_ID), part_object_id_(OB_INVALID_ID) {}
  ObTransferPartInfo(const ObObjectID &table_id, const ObObjectID &part_object_id) :
      table_id_(table_id), part_object_id_(part_object_id) {}
  ~ObTransferPartInfo() = default;
  void reset();
  bool is_valid() const { return OB_INVALID_ID != table_id_ && OB_INVALID_ID != part_object_id_; }
  const ObObjectID &table_id() const { return table_id_; }
  const ObObjectID &part_object_id() const { return part_object_id_; }

  // define init to avoid initializing variables separately
  int init(const ObObjectID &table_id, const ObObjectID &part_object_id);
  // init by partition schema
  int init(const schema::ObBasePartition &part_schema);

  ///////////////////////// display string related ////////////////////////
  // "table_id:part_id" max length: 20 + 20 + ':' + '\0'
  int64_t max_display_str_len() const { return 42; }
  // parse from string "table_id:part_id"
  int parse_from_display_str(const common::ObString &str);
  // generate string "table_id:part_id"
  // NOTE: can not include commas ','
  int to_display_str(char *buf, const int64_t len, int64_t &pos) const;

  bool operator==(const ObTransferPartInfo &other) const;

  TO_STRING_KV(K_(table_id), K_(part_object_id));
private:
    ObObjectID table_id_;
    ObObjectID part_object_id_;	// It means part_id for level one partition or subpart_id for subpartition.
};

typedef ObDisplayList<ObTransferPartInfo> ObTransferPartList;

struct ObDisplayTabletID final : public ObDisplayType
{
  OB_UNIS_VERSION(1);
public:
  ObDisplayTabletID() : tablet_id_() {}
  explicit ObDisplayTabletID(const ObTabletID &tablet_id) : tablet_id_(tablet_id) {}
  ~ObDisplayTabletID() = default;
  void reset() { tablet_id_.reset(); }
  bool is_valid() const { return tablet_id_.is_valid(); }
  const ObTabletID &tablet_id() const { return tablet_id_; }

  ///////////////////////// display string related ////////////////////////
  // "tablet_id" max length: 20 + '\0'
  int64_t max_display_str_len() const { return 21; }
  // parse from string "tablet_id"
  int parse_from_display_str(const common::ObString &str);
  // generate string "tablet_id"
  int to_display_str(char *buf, const int64_t len, int64_t &pos) const;

  bool operator==(const ObDisplayTabletID &other) const { return tablet_id_ == other.tablet_id_; };
  TO_STRING_KV(K_(tablet_id));
private:
  ObTabletID tablet_id_;
};

typedef ObDisplayList<ObDisplayTabletID> ObDisplayTabletList;

/////////////// ObTransferTask related ///////////////
// used to display the task extra info in the comment column of __all_transfer_task
// when you add comment, you need to add corresponding string to TRANSFER_TASK_COMMENT_ARRAY[]
enum ObTransferTaskComment
{
  EMPTY_COMMENT = 0,
  WAIT_FOR_MEMBER_LIST = 1,
  TASK_COMPLETED_AS_NO_VALID_PARTITION = 2,
  TASK_CANCELED = 3,
  TRANSACTION_TIMEOUT = 4,
  INACTIVE_SERVER_IN_MEMBER_LIST = 5,
  WAIT_DUE_TO_LAST_FAILURE = 6,
  MAX_COMMENT
};

//for ObTransferTaskComment
const char *transfer_task_comment_to_str(const ObTransferTaskComment &comment);
ObTransferTaskComment str_to_transfer_task_comment(const common::ObString &str);

// Represents a row in __all_transfer_task
class ObTransferTask
{
public:
  struct TaskStatus final
  {
  public:
    TaskStatus() : task_id_(), status_() {}
    ~TaskStatus() {}
    int init(const ObTransferTaskID task_id, const ObTransferStatus &status);
    void reset();
    TaskStatus &operator=(const TaskStatus &other);
    bool is_valid() const { return task_id_.is_valid() && status_.is_valid(); }
    const ObTransferTaskID &get_task_id() const { return task_id_; }
    const ObTransferStatus &get_status() const { return status_; }

    TO_STRING_KV(K_(task_id), K_(status));
  private:
    ObTransferTaskID task_id_;
    ObTransferStatus status_;
  };

public:
  ObTransferTask();
  virtual ~ObTransferTask() {}
  void reset();

  // init by necessary info, other members take default values
  int init(
  const ObTransferTaskID task_id,
  const ObLSID &src_ls,
  const ObLSID &dest_ls,
  const ObTransferPartList &part_list,
  const ObTransferStatus &status,
  const common::ObCurTraceId::TraceId &trace_id,
  const ObBalanceTaskID balance_task_id,
  const uint64_t data_version);

  // init all members
  int init(
  const ObTransferTaskID task_id,
  const ObLSID &src_ls,
  const ObLSID &dest_ls,
  const ObString &part_list_str,
  const ObString &not_exist_part_list_str,
  const ObString &lock_conflict_part_list_str,
  const ObString &table_lock_tablet_list_str,
  const ObString &tablet_list_str,
  const share::SCN &start_scn,
  const share::SCN &finish_scn,
  const ObTransferStatus &status,
  const common::ObCurTraceId::TraceId &trace_id,
  const int result,
  const ObTransferTaskComment &comment,
  const ObBalanceTaskID balance_task_id,
  const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
  const uint64_t data_version);

  int assign(const ObTransferTask &other);
  bool is_valid() const;

  void set_status(const ObTransferStatus &status) { status_ = status; }
  void set_start_scn(const share::SCN &start_scn) { start_scn_ = start_scn; }
  void set_finish_scn(const share::SCN &finish_scn) { finish_scn_ = finish_scn; }
  void set_result(const int32_t result) { result_ = result; }

  ObTransferTaskID get_task_id() const { return task_id_; }
  const ObLSID &get_src_ls() const { return src_ls_; }
  const ObLSID &get_dest_ls() const { return dest_ls_; }
  const ObTransferPartList &get_part_list() const { return part_list_; }
  ObTransferPartList &get_part_list() { return part_list_; }
  const ObTransferPartList &get_not_exist_part_list() const { return not_exist_part_list_; }
  const ObTransferPartList &get_lock_conflict_part_list() const { return lock_conflict_part_list_; }
  const ObDisplayTabletList &get_table_lock_tablet_list() const { return table_lock_tablet_list_; }
  const ObTransferTabletList &get_tablet_list() const { return tablet_list_; }
  ObTransferTabletList &get_tablet_list() { return tablet_list_; }
  const share::SCN &get_start_scn() const { return start_scn_; }
  const share::SCN &get_finish_scn() const { return finish_scn_; }
  const ObTransferStatus &get_status() const { return status_; }
  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  int32_t get_result() const { return result_; }
  const ObTransferTaskComment &get_comment() const { return comment_; }
  ObBalanceTaskID get_balance_task_id() const { return balance_task_id_; }
  const transaction::tablelock::ObTableLockOwnerID &get_table_lock_owner_id() const
  {
    return table_lock_owner_id_;
  }
  uint64_t get_data_version() const { return data_version_; }

  TO_STRING_KV(K_(task_id), K_(src_ls), K_(dest_ls), K_(part_list),
      K_(not_exist_part_list), K_(lock_conflict_part_list), K_(table_lock_tablet_list), K_(tablet_list), K_(start_scn), K_(finish_scn),
      K_(status), K_(trace_id), K_(result), K_(comment), K_(balance_task_id), K_(table_lock_owner_id), K_(data_version));

private:
  ObTransferTaskID task_id_;
  share::ObLSID src_ls_;
  share::ObLSID dest_ls_;
  ObTransferPartList part_list_;
  ObTransferPartList not_exist_part_list_;
  ObTransferPartList lock_conflict_part_list_;
  ObDisplayTabletList table_lock_tablet_list_;
  ObTransferTabletList tablet_list_;
  share::SCN start_scn_;
  share::SCN finish_scn_;
  ObTransferStatus status_;
  common::ObCurTraceId::TraceId trace_id_;
  int32_t result_;
  ObTransferTaskComment comment_;
  ObBalanceTaskID balance_task_id_;
  transaction::tablelock::ObTableLockOwnerID table_lock_owner_id_;
  uint64_t data_version_; // for upgrade compatibility
};

struct ObTransferTaskInfo final
{
  ObTransferTaskInfo();
  ~ObTransferTaskInfo() = default;
  void reset();
  bool is_valid() const;
  int convert_from(const uint64_t tenant_id, const ObTransferTask &task);
  int assign(const ObTransferTaskInfo &task_info);
  int fill_tablet_ids(ObIArray<ObTabletID> &tablet_ids) const;
  TO_STRING_KV(K_(tenant_id), K_(src_ls_id), K_(dest_ls_id), K_(task_id), K_(trace_id),
      K_(status), K_(table_lock_owner_id), K_(table_lock_tablet_list), K_(tablet_list),
      K_(start_scn), K_(finish_scn), K_(result), K_(data_version));

  uint64_t tenant_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  ObTransferTaskID task_id_;
  common::ObCurTraceId::TraceId trace_id_;
  ObTransferStatus status_;
  transaction::tablelock::ObTableLockOwnerID table_lock_owner_id_;
  ObDisplayTabletList table_lock_tablet_list_;
  ObArray<ObTransferTabletInfo> tablet_list_;
  share::SCN start_scn_;
  share::SCN finish_scn_;
  int32_t result_;
  uint64_t data_version_; // for upgrade compatibility
  DISALLOW_COPY_AND_ASSIGN(ObTransferTaskInfo);
};

// remove discarded_list from original_list
template <typename LIST>
static int remove_from_list(LIST &original_list, const LIST &discarded_list)
{
  int ret = OB_SUCCESS;
  for(int64_t i = discarded_list.count() - 1; i >= 0 && OB_SUCC(ret); --i) {
    for(int64_t j = original_list.count() - 1; j >= 0 && OB_SUCC(ret); --j) {
      if (discarded_list.at(i) == original_list.at(j)) {
        if (OB_FAIL(original_list.remove(j))) {
          COMMON_LOG(WARN, "remove failed", KR(ret), K(i), K(j), K(discarded_list), K(original_list));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

// part table lock on tablet needs to be moved from src_ls to dest_ls when tablets transfer
class ObTransferLockUtil
{
public:
  // Used to move part table lock on tablet. This function is called in transfer start transaction.
  static int lock_tablet_on_dest_ls_for_table_lock(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const ObLSID &dest_ls,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
      const ObDisplayTabletList &table_lock_tablet_list);
  // Used to move part table lock on tablet. This function is called in transfer finish transaction.
  static int unlock_tablet_on_src_ls_for_table_lock(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const ObLSID &src_ls,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
      const ObDisplayTabletList &table_lock_tablet_list);
private:
  static int process_table_lock_on_tablets_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
      const ObDisplayTabletList &table_lock_tablet_list,
      transaction::tablelock::ObLockAloneTabletRequest &lock_arg);
};

} // end namespace share
} // end namespace oceanbase
#endif
