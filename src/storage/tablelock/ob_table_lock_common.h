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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_
#include "common/ob_simple_iterator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/scn.h"
#include "share/ob_common_id.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace common
{
  class ObTabletID;
}
namespace transaction
{
namespace tablelock
{
enum class ObTableLockPriority : int8_t
{
  INVALID = -1,
  HIGH1 = 0,
  HIGH2 = 10,
  NORMAL = 20,
  LOW = 30,
};

// Lock compatibility matrix:
//
// +---------------------+-----------+---------------+-------+---------------------+-----------+
// |                     | ROW SHARE | ROW EXCLUSIVE | SHARE | SHARE ROW EXCLUSIVE | EXCLUSIVE |
// +---------------------+-----------+---------------+-------+---------------------+-----------+
// | ROW SHARE           | Y         | Y             | Y     | Y                   | X         |
// | ROW EXCLUSIVE       | Y         | Y             | X     | X                   | X         |
// | SHARE               | Y         | X             | Y     | X                   | X         |
// | SHARE ROW EXCLUSIVE | Y         | X             | X     | X                   | X         |
// | EXCLUSIVE           | X         | X             | X     | X                   | X         |
// +---------------------+-----------+---------------+-------+---------------------+-----------+

typedef unsigned char ObTableLockMode;
static const char TABLE_LOCK_MODE_COUNT = 5;

static const unsigned char NO_LOCK             = 0x0; // binary 0000
static const unsigned char ROW_SHARE           = 0x8; // binary 1000
static const unsigned char ROW_EXCLUSIVE       = 0x4; // binary 0100
static const unsigned char SHARE               = 0x2; // binary 0010
static const unsigned char SHARE_ROW_EXCLUSIVE = 0x6; // binary 0110, SHARE | ROW_EXCLUSIVE
static const unsigned char EXCLUSIVE           = 0x1; // binary 0001
static const unsigned char MAX_LOCK_MODE       = 0xf;

// Each item occupies 4 bits, stand for ROW SHARE, ROW EXCLUSIVE, SHARE, EXCLUSIVE.
static const unsigned char compatibility_matrix[] = { 0x0, /* EXCLUSIVE    : 0000 */
                                                      0xa, /* SHARE        : 1010 */
                                                      0xc, /* ROW EXCLUSIVE: 1100 */
                                                      0xe  /* ROW SHARE    : 1110 */ };

static inline
int lock_mode_to_string(const ObTableLockMode lock_mode,
                        char *str,
                        const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (NO_LOCK == lock_mode) {
    strncpy(str ,"N", str_len);
  } else if (ROW_SHARE == lock_mode) {
    strncpy(str ,"RS", str_len);
  } else if (ROW_EXCLUSIVE == lock_mode) {
    strncpy(str ,"RX", str_len);
  } else if (SHARE == lock_mode) {
    strncpy(str ,"S", str_len);
  } else if (SHARE_ROW_EXCLUSIVE == lock_mode) {
    strncpy(str ,"SRX", str_len);
  } else if (EXCLUSIVE == lock_mode) {
    strncpy(str ,"X", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static inline
ObTableLockMode get_lock_mode_from_oracle_mode(const int64_t oracle_lock_mode)
{
  ObTableLockMode ob_lock_mode = MAX_LOCK_MODE;
  switch (oracle_lock_mode) {
  case 1: { ob_lock_mode = NO_LOCK; break; }
  case 2: { ob_lock_mode = ROW_SHARE; break; }
  case 3: { ob_lock_mode = ROW_EXCLUSIVE; break; }
  case 4: { ob_lock_mode = SHARE; break; }
  case 5: { ob_lock_mode = SHARE_ROW_EXCLUSIVE; break; }
  case 6: { ob_lock_mode = EXCLUSIVE; break; }
  default: { ob_lock_mode = MAX_LOCK_MODE; }
  }
  return ob_lock_mode;
}

static inline
bool is_lock_mode_valid(const ObTableLockMode lock_mode)
{
  return lock_mode < MAX_LOCK_MODE;
}

static inline
int get_index_by_lock_mode(const ObTableLockMode &lock_mode)
{
  int index = -1;
  if (is_lock_mode_valid(lock_mode)) {
    index = lock_mode >> 1;
  }
  return index;
}

static inline
ObTableLockMode get_lock_mode_by_index(const int index)
{
  ObTableLockMode lock_mode = MAX_LOCK_MODE;
  if (index >= 0 && index < TABLE_LOCK_MODE_COUNT) {
    // EXCLUSIVE is 0001, the lowest bit will be lost during left shift
    if (index == 0) {
      lock_mode = EXCLUSIVE;
    } else {
      lock_mode = index << 1;
    }
  }
  return lock_mode;
}

static inline
bool request_lock(ObTableLockMode curr_lock,
                  ObTableLockMode new_lock,
                  int64_t &conflict_modes)
{
  if (!is_lock_mode_valid(curr_lock) || !is_lock_mode_valid(new_lock)) {
    return false;
  } else {
    int64_t index = 0;
    int64_t compat = 0xf;
    conflict_modes = 0;

    while (curr_lock > 0 && compat > 0) {
      if (curr_lock & 1) {
        compat &= compatibility_matrix[index];
        // if new lock conflict with this lock mode.
        if (!(new_lock == (compatibility_matrix[index] & new_lock))) {
          conflict_modes |= (1 << index);
        }
      }
      curr_lock >>= 1;
      index += 1;
    }
    return new_lock == (compat & new_lock);
  }
}

enum ObTableLockOpType : char
{
  UNKNOWN_TYPE = 0,
  IN_TRANS_DML_LOCK = 1,  // will be unlock if we do callback
  OUT_TRANS_LOCK = 2, // will be unlock use OUT_TRANS_UNLOCK
  OUT_TRANS_UNLOCK = 3,
  IN_TRANS_COMMON_LOCK = 4,
  MAX_VALID_LOCK_OP_TYPE,
};

static inline
int lock_op_type_to_string(const ObTableLockOpType op_type,
                           char *str,
                           const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (UNKNOWN_TYPE == op_type) {
    strncpy(str ,"UNKNOWN_TYPE", str_len);
  } else if (IN_TRANS_DML_LOCK == op_type) {
    strncpy(str ,"IN_TRANS_DML_LOCK", str_len);
  } else if (OUT_TRANS_LOCK == op_type) {
    strncpy(str ,"OUT_TRANS_LOCK", str_len);
  } else if (OUT_TRANS_UNLOCK == op_type) {
    strncpy(str ,"OUT_TRANS_UNLOCK", str_len);
  } else if (IN_TRANS_COMMON_LOCK == op_type) {
    strncpy(str ,"IN_TRANS_COMMON_LOCK", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

enum ObTableLockOpStatus : char
{
  UNKNOWN_STATUS = 0,
  LOCK_OP_DOING = 1,
  LOCK_OP_COMPLETE
};

static inline
int lock_op_status_to_string(const ObTableLockOpStatus op_status,
                             char *str,
                             const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (UNKNOWN_STATUS == op_status) {
    strncpy(str ,"UNKNOWN", str_len);
  } else if (LOCK_OP_DOING == op_status) {
    strncpy(str ,"DOING", str_len);
  } else if (LOCK_OP_COMPLETE == op_status) {
    strncpy(str ,"COMPLETE", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

static inline
bool is_op_type_valid(const ObTableLockOpType type)
{
  return (type < MAX_VALID_LOCK_OP_TYPE &&
          type > UNKNOWN_TYPE);
}

static inline
bool is_out_trans_op_type(const ObTableLockOpType type)
{
  return (type == OUT_TRANS_LOCK ||
          type == OUT_TRANS_UNLOCK);
}

static inline
bool is_in_trans_common_lock_op_type(const ObTableLockOpType type)
{
  return (IN_TRANS_COMMON_LOCK == type);
}

static inline
bool is_in_trans_dml_lock_op_type(const ObTableLockOpType type)
{
  return (IN_TRANS_DML_LOCK == type);
}

static inline
bool is_op_status_valid(const ObTableLockOpStatus status)
{
  return (status > UNKNOWN_STATUS && status <= LOCK_OP_COMPLETE);
}

enum class ObLockOBJType : char
{
  OBJ_TYPE_INVALID = 0,
  OBJ_TYPE_TABLE = 1, // table
  OBJ_TYPE_TABLET = 2, // tablet
  OBJ_TYPE_COMMON_OBJ = 3, // common_obj
  OBJ_TYPE_LS = 4,     // for ls
  OBJ_TYPE_TENANT = 5, // for tenant
  OBJ_TYPE_EXTERNAL_TABLE_REFRESH = 6, // for external table
  OBJ_TYPE_ONLINE_DDL_TABLE = 7, // online ddl table
  OBJ_TYPE_ONLINE_DDL_TABLET = 8, // online ddl tablets
  OBJ_TYPE_DATABASE_NAME = 9,   // for database related ddl
  OBJ_TYPE_OBJECT_NAME = 10,     // for obj related ddl
  OBJ_TYPE_DBMS_LOCK = 11,  // for dbms lock
  OBJ_TYPE_MATERIALIZED_VIEW = 12, // for materialized view operations
  OBJ_TYPE_MYSQL_LOCK_FUNC = 13,  // for mysql lock function
  OBJ_TYPE_MAX
};

static inline
int lock_obj_type_to_string(const ObLockOBJType obj_type,
                            char *str,
                            const int64_t str_len)
{
  int ret = OB_SUCCESS;
  switch (obj_type) {
  case ObLockOBJType::OBJ_TYPE_TABLE: {
    strncpy(str, "TABLE", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_TABLET: {
    strncpy(str, "TABLET", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_COMMON_OBJ: {
    strncpy(str, "COMMON_OBJ", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_LS: {
    strncpy(str, "LS", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_TENANT: {
    strncpy(str, "TENANT", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_EXTERNAL_TABLE_REFRESH: {
    strncpy(str, "EXTERNAL_TABLE_REFRES", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLE: {
    strncpy(str, "ONLINE_DDL_TABLE", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLET: {
    strncpy(str, "ONLINE_DDL_TABLET", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_DATABASE_NAME: {
    strncpy(str, "DATABASE_NAME", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_OBJECT_NAME: {
    strncpy(str, "OBJECT_NAME", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_DBMS_LOCK: {
    strncpy(str, "DBMS_LOCK", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_MATERIALIZED_VIEW: {
    strncpy(str, "MATERIALIZED_VIEW", str_len);
    break;
  }
  case ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC: {
    strncpy(str, "MYSQL_LOCK_FUNC", str_len);
    break;
  }
  default: {
    strncpy(str, "UNKNOWN", str_len);
  }
  }
  return ret;
}

static inline
bool is_lock_obj_type_valid(const ObLockOBJType &type)
{
  return (type > ObLockOBJType::OBJ_TYPE_INVALID &&
          type < ObLockOBJType::OBJ_TYPE_MAX);
}

static inline
bool is_tablet_obj_type(const ObLockOBJType &type)
{
  return (ObLockOBJType::OBJ_TYPE_TABLET == type);
}

struct ObLockID final
{
public:
  ObLockID()
    : obj_type_(ObLockOBJType::OBJ_TYPE_INVALID),
    obj_id_(common::OB_INVALID_ID),
    hash_value_(0) {}
  uint64_t hash() const
  { return hash_value_; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  uint64_t inner_hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&obj_type_, sizeof(obj_type_), hash_val);
    hash_val = murmurhash(&obj_id_, sizeof(obj_id_), hash_val);
    return hash_val;
  }
  bool is_valid() const
  {
    return (common::is_valid_id(obj_id_) &&
            is_lock_obj_type_valid(obj_type_));
  }
  int compare(const ObLockID  &other) const
  {
    int cmp_ret = 0;
    if (obj_type_ > other.obj_type_) {
      cmp_ret = 1;
    } else if (obj_type_ < other.obj_type_) {
      cmp_ret = -1;
    } else if (obj_id_ > other.obj_id_) {
      cmp_ret = 1;
    } else if (obj_id_ < other.obj_id_) {
      cmp_ret = -1;
    }
    return cmp_ret;
  }
  bool operator==(const ObLockID &other) const
  {
   return (0 == compare(other));
  }
  bool operator<(const ObLockID &other) const { return -1 == compare(other); }
  bool operator!=(const ObLockID &other) const { return !operator==(other); }
  bool is_tablet_lock() const { return is_tablet_obj_type(obj_type_); }
  // convert to tablet id, if it is not a valid tablet id, return error code.
  int convert_to(common::ObTabletID &tablet_id) const;
  int set(const ObLockOBJType &type, const uint64_t obj_id);
  void reset()
  {
    obj_type_ = ObLockOBJType::OBJ_TYPE_INVALID;
    obj_id_ = common::OB_INVALID_ID;
    hash_value_ = 0;
  }
  TO_STRING_KV(K_(obj_type), K_(obj_id));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  ObLockOBJType obj_type_;
  uint64_t obj_id_;
  uint64_t hash_value_;
};
int get_lock_id(const ObLockOBJType obj_type,
                const uint64_t obj_id,
                ObLockID &lock_id);
int get_lock_id(const uint64_t table_id,
                ObLockID &lock_id);
int get_lock_id(const common::ObTabletID &tablet,
                ObLockID &lock_id);
// typedef share::ObCommonID ObTableLockOwnerID;

enum class ObLockOwnerType : unsigned char {
  DEFAULT_OWNER_TYPE    = 0,
  SESS_ID_OWNER_TYPE    = 1,

  // make sure this is smaller than INVALID_OWNER_TYPE
  MAX_OWNER_TYPE,

  INVALID_OWNER_TYPE    = 255,
};

static inline
bool is_lock_owner_type_valid(const ObLockOwnerType &type)
{
  return (type < ObLockOwnerType::MAX_OWNER_TYPE);
}

class ObTableLockOwnerID
{
public:
  static const int64_t INVALID_ID = -1;
  static const int64_t INVALID_RAW_OWNER_ID = ((1L << 54) - 1);
  static const int64_t CLIENT_SESS_CREATE_TS_BIT = 22;
  static const int64_t CLIENT_SESS_CREATE_TS_MASK = (1L << CLIENT_SESS_CREATE_TS_BIT) - 1;
  static const int64_t CLIENT_SESS_ID_BIT = 32;
  static const int64_t CLIENT_SESS_ID_MASK = (1L << CLIENT_SESS_ID_BIT) - 1;
  static const int64_t MAX_VALID_RAW_OWNER_ID = INVALID_RAW_OWNER_ID - 1;

  ObTableLockOwnerID() : pack_(INVALID_ID) {}
  ObTableLockOwnerID(const ObTableLockOwnerID &other) : pack_(other.pack_) {}
  ~ObTableLockOwnerID() { reset(); }
public:
  int64_t raw_value() const { return pack_; }
  int64_t id() const { return id_; }
  bool is_session_id_owner() const { return type_ == static_cast<unsigned char>(ObLockOwnerType::SESS_ID_OWNER_TYPE); }


  void reset() { pack_ = INVALID_ID; }
  bool is_valid() const
  {
    return (INVALID_ID != pack_ &&
            static_cast<int64_t>(ObLockOwnerType::INVALID_OWNER_TYPE) != type_ &&
            INVALID_RAW_OWNER_ID != id_);
  }
  static ObTableLockOwnerID default_owner();
  static ObTableLockOwnerID get_owner_by_value(const int64_t packet_id);
  void set_default()
  { pack_ = 0; }
  // without check whether it is valid.
  int convert_from_value(const int64_t packed_id);
  // check valid.
  int convert_from_value(const ObLockOwnerType owner_type, const int64_t raw_owner_id);
  int convert_from_client_sessid(const uint32_t client_sessid, const uint64_t client_sess_create_ts);
  int convert_to_sessid(uint32_t &sessid) const;
  // assignment
  ObTableLockOwnerID &operator=(const ObTableLockOwnerID &other)
  { pack_ = other.pack_; return *this; }

  // compare operator
  bool operator == (const ObTableLockOwnerID &other) const
  { return pack_ == other.pack_; }
  bool operator >  (const ObTableLockOwnerID &other) const
  { return pack_ > other.pack_; }
  bool operator != (const ObTableLockOwnerID &other) const
  { return pack_ != other.pack_; }
  bool operator <  (const ObTableLockOwnerID &other) const
  { return pack_ < other.pack_; }
  bool operator <= (const ObTableLockOwnerID &other) const
  { return pack_ <= other.pack_; }
  bool operator >= (const ObTableLockOwnerID &other) const
  { return pack_ >= other.pack_; }
  int compare(const ObTableLockOwnerID &other) const
  {
   if (pack_ == other.pack_) {
     return 0;
   } else if (pack_ < other.pack_) {
     return -1;
   } else {
     return 1;
   }
  }

  uint64_t hash() const
  { return pack_; }
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(pack), K_(type), K_(id), K_(reserved), K_(valid_flag));
private:
  union {
    struct {
      int64_t id_             : 54;
      int64_t type_           : 8;
      int64_t reserved_       : 1;
      int64_t valid_flag_     : 1;
    };
    int64_t pack_;
  };
};

struct ObTableLockOp
{
public:
  static constexpr int64_t MAX_SERIALIZE_SIZE = 256;
  OB_UNIS_VERSION(1);
public:
  ObTableLockOp()
    : lock_id_(),
      lock_mode_(NO_LOCK),
      owner_id_(ObTableLockOwnerID::default_owner()),
      create_trans_id_(),
      op_type_(UNKNOWN_TYPE),
      lock_op_status_(UNKNOWN_STATUS),
      lock_seq_no_(),
      commit_version_(),
      commit_scn_(),
      create_timestamp_(0),
      create_schema_version_(-1)
  {}
  ObTableLockOp(
      const ObLockID &lock_id,
      const ObTableLockMode lock_mode,
      const ObTableLockOwnerID &owner_id,
      const ObTransID &trans_id,
      const ObTableLockOpType op_type,
      const ObTableLockOpStatus lock_op_status,
      const ObTxSEQ seq_no,
      const int64_t create_timestamp,
      const int64_t create_schema_version) :
      lock_id_(),
      lock_mode_(NO_LOCK),
      owner_id_(ObTableLockOwnerID::default_owner()),
      create_trans_id_(),
      op_type_(UNKNOWN_TYPE),
      lock_op_status_(UNKNOWN_STATUS),
      lock_seq_no_(),
      commit_version_(),
      commit_scn_(),
      create_timestamp_(0),
      create_schema_version_(-1)
  {
    // here, ensure lock-callback was dispatched to single callback-list
    // forcedly set the seq_no's branch to zero
    ObTxSEQ seq_no2 = seq_no;
    seq_no2.set_branch(0);
    set(lock_id,
        lock_mode,
        owner_id,
        trans_id,
        op_type,
        lock_op_status,
        seq_no2,
        create_timestamp,
        create_schema_version);
  }
  void set(
      const ObLockID &lock_id,
      const ObTableLockMode lock_mode,
      const ObTableLockOwnerID &owner_id,
      const ObTransID &trans_id,
      const ObTableLockOpType op_type,
      const ObTableLockOpStatus lock_op_status,
      const ObTxSEQ seq_no,
      const int64_t create_timestamp,
      const int64_t create_schema_version);
  bool is_valid() const;
  bool is_out_trans_lock_op() const
  { return is_out_trans_op_type(op_type_); }
  bool need_register_callback() const
  { return !is_out_trans_op_type(op_type_); }
  bool need_multi_source_data() const
  { return is_out_trans_op_type(op_type_); }
  bool need_record_lock_op() const
  {
    return (is_out_trans_op_type(op_type_) ||
            is_need_record_lock_mode_() ||
            is_in_trans_common_lock_op_type(op_type_));
  }
  bool is_dml_lock_op() const
  {
    return is_in_trans_dml_lock_op_type(op_type_);
  }
  bool need_wakeup_waiter() const
  {
    return (is_out_trans_op_type(op_type_) ||
            is_in_trans_common_lock_op_type(op_type_));
  }
  bool need_replay_or_recover(const ObTableLockOp &lock_op) const;

  bool is_tablet_lock(const ObTabletID &tablet_id) {
    return lock_id_.is_tablet_lock() && lock_id_.obj_id_ == tablet_id.id();
  }
private:
  bool is_need_record_lock_mode_() const
  {
    return (lock_mode_ == SHARE ||
            lock_mode_ == SHARE_ROW_EXCLUSIVE ||
            lock_mode_ == EXCLUSIVE);
  }
public:
  TO_STRING_KV(K_(lock_id), K_(lock_mode), K_(owner_id), K_(create_trans_id),
               K_(op_type), K_(lock_op_status), K_(lock_seq_no),
               K_(commit_version), K_(commit_scn), K_(create_timestamp),
               K_(create_schema_version));

  ObLockID lock_id_;
  ObTableLockMode lock_mode_;
  ObTableLockOwnerID owner_id_;
  ObTransID create_trans_id_;
  ObTableLockOpType op_type_;
  ObTableLockOpStatus lock_op_status_;
  ObTxSEQ lock_seq_no_;
  share::SCN commit_version_;
  share::SCN commit_scn_;
  // used to check whether a trans modify before a schema_version or timestamp.
  int64_t create_timestamp_;
  int64_t create_schema_version_;
};

typedef common::ObSEArray<ObTableLockOp, 10, TransModulePageAllocator> ObTableLockOpArray;
struct ObTableLockInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTableLockInfo() : table_lock_ops_(), max_durable_scn_() {}
  void reset();
  TO_STRING_KV(K_(table_lock_ops), K_(max_durable_scn));
  ObTableLockOpArray table_lock_ops_;
  share::SCN max_durable_scn_;
};

static inline
bool is_need_retry_unlock_error(int err)
{
  return (err == OB_OBJ_LOCK_NOT_COMPLETED ||
          err == OB_OBJ_UNLOCK_CONFLICT);
}

struct ObSimpleIteratorModIds
{
  static constexpr const char OB_OBJ_LOCK[] = "obj_lock";
  static constexpr const char OB_OBJ_LOCK_MAP[] = "obj_lock_map";
};

// Is used to store and traverse all lock id
typedef common::ObSimpleIterator<ObLockID, ObSimpleIteratorModIds::OB_OBJ_LOCK_MAP, 16> ObLockIDIterator;
// Is used to store and traverse all lock op
typedef common::ObSimpleIterator<ObTableLockOp, ObSimpleIteratorModIds::OB_OBJ_LOCK, 16> ObLockOpIterator;

// the threshold of timeout interval which will enable the deadlock avoid.
static const int64_t MIN_DEADLOCK_AVOID_TIMEOUT_US = 60 * 1000 * 1000; // 1 min
bool is_deadlock_avoid_enabled(const bool is_from_sql, const int64_t expire_time);

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_ */
