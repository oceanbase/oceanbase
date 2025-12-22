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

// KNN means print the value x with a new name
#define KNN(name, x) #name, ::oceanbase::common::check_char_array(x)
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
class ObTableLockOwnerID;

enum class ObTableLockPriority : int8_t
{
  INVALID = -1,
#define DEF_LOCK_PRIORITY(n, type)              \
  type = n,
#include "ob_table_lock_def.h"
#undef DEF_LOCK_PRIORITY
};
const char *get_name(const ObTableLockPriority intype);

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

#define DEF_LOCK_MODE(n, type, name)            \
static const unsigned char type = n;
#include "ob_table_lock_def.h"
#undef DEF_LOCK_MODE

static const unsigned char MAX_LOCK_MODE       = 0xf;

// Each item occupies 4 bits, stand for ROW SHARE, ROW EXCLUSIVE, SHARE, EXCLUSIVE.
static const unsigned char compatibility_matrix[] = { 0x0, /* EXCLUSIVE    : 0000 */
                                                      0xa, /* SHARE        : 1010 */
                                                      0xc, /* ROW EXCLUSIVE: 1100 */
                                                      0xe  /* ROW SHARE    : 1110 */ };

const char *get_name(const ObTableLockMode intype);
static inline
int lock_mode_to_string(const ObTableLockMode lock_mode,
                        char *str,
                        const int64_t str_len)
{
  int ret = OB_SUCCESS;
  strncpy(str, get_name(lock_mode), str_len);
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
bool is_need_lock_tablet_mode(const ObTableLockMode &lock_mode)
{
  return SHARE == lock_mode || SHARE_ROW_EXCLUSIVE == lock_mode || EXCLUSIVE == lock_mode;
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

// check whether the first lock mode is equal to or greater than the second
// rule: EXCLUSIVE > SHARE > ROW_SHARE > NO_LOCK
// rule: EXCLUSIVE > ROW_EXCLUSIVE >= ROW_SHARE > NO_LOCK
static inline
bool is_equal_or_greater_lock_mode(
    const ObTableLockMode first,
    const ObTableLockMode second)
{
  bool ret_bool = false;
  if (!is_lock_mode_valid(first) || !is_lock_mode_valid(second)) {
    ret_bool = false;
  } else if (second == NO_LOCK) {
    ret_bool = true;
  } else if (first & EXCLUSIVE) {
    ret_bool = true;
  } else if (first & SHARE) {
    if ((second & EXCLUSIVE) || (second & ROW_EXCLUSIVE)) {
      // return false
    } else {
      ret_bool = true;
    }
  } else if (first & ROW_EXCLUSIVE) {
    if ((second & SHARE) || (second & EXCLUSIVE)) {
      // return false
    } else {
      ret_bool = true;
    }
  } else if (first & ROW_SHARE) {
    if ((second & SHARE)
        || (second & EXCLUSIVE)
        || (second & ROW_EXCLUSIVE)) {
      // return false
    } else {
      ret_bool = true;
    }
  }
  return ret_bool;
}

enum ObTableLockOpType : char
{
  UNKNOWN_TYPE = 0,
#define DEF_LOCK_OP_TYPE(n, type)        \
  type = n,
#include "ob_table_lock_def.h"
#undef DEF_LOCK_OP_TYPE
  MAX_VALID_LOCK_OP_TYPE,
};

const char *get_name(const ObTableLockOpType intype);
static inline
int lock_op_type_to_string(const ObTableLockOpType op_type,
                           char *str,
                           const int64_t str_len)
{
  int ret = OB_SUCCESS;
  strncpy(str, get_name(op_type), str_len);
  return ret;
}

enum ObTableLockOpStatus : char
{
  UNKNOWN_STATUS = 0,

#define DEF_LOCK_OP_STATUS(n, type)             \
  LOCK_OP_##type = n,
#include "ob_table_lock_def.h"
#undef DEF_LOCK_OP_STATUS
};

const char *get_name(const ObTableLockOpStatus intype);
static inline
int lock_op_status_to_string(const ObTableLockOpStatus op_status,
                             char *str,
                             const int64_t str_len)
{
  int ret = OB_SUCCESS;
  strncpy(str, get_name(op_status), str_len);
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

#define DEF_OBJ_TYPE(n, type) \
  OBJ_TYPE_##type = n,
#include "ob_table_lock_def.h"
#undef DEF_OBJ_TYPE

  OBJ_TYPE_MAX
};

const char *get_name(const ObLockOBJType obj_type);
static inline
int lock_obj_type_to_string(const ObLockOBJType obj_type,
                            char *str,
                            const int64_t str_len)
{
  int ret = OB_SUCCESS;
  strncpy(str, get_name(obj_type), str_len);
  return ret;
}

static inline int lock_priority_to_string(const ObTableLockPriority priority, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  switch (priority) {
  case ObTableLockPriority::LOW: {
    strncpy(str, "LOW", str_len);
    break;
  }
  case ObTableLockPriority::NORMAL: {
    strncpy(str, "NORMAL", str_len);
    break;
  }
  case ObTableLockPriority::HIGH1: {
    strncpy(str, "HIGH1", str_len);
    break;
  }
  case ObTableLockPriority::HIGH2: {
    strncpy(str, "HIGH2", str_len);
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
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
  TO_STRING_KV(KNN("type", obj_type_),
               "type_str", get_name(obj_type_),
               KNN("id", obj_id_));
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
int get_lock_id(const ObIArray<ObTabletID> &tablets,
                ObIArray<ObLockID> &lock_ids);
// typedef share::ObCommonID ObTableLockOwnerID;

enum class ObLockOwnerType : unsigned char {
#define DEF_LOCK_OWNER_TYPE(n, type)                    \
  type##_OWNER_TYPE = n,
#include "ob_table_lock_def.h"
#undef DEF_LOCK_OWNER_TYPE
  // make sure this is smaller than INVALID_OWNER_TYPE
  MAX_OWNER_TYPE,

  INVALID_OWNER_TYPE    = 255,
};

const char *get_name(const ObLockOwnerType intype);
static inline
bool is_lock_owner_type_valid(const ObLockOwnerType &type)
{
  return (type < ObLockOwnerType::MAX_OWNER_TYPE);
}

class ObTableLockOwnerID
{
public:
  static const int64_t MAGIC_NUM = -0xABC;
  static const int64_t INVALID_ID = -1;
  static const int64_t INVALID_RAW_OWNER_ID = ((1L << 54) - 1);
  static const int64_t CLIENT_SESS_CREATE_TS_BIT = 22;
  static const int64_t CLIENT_SESS_CREATE_TS_MASK = (1L << CLIENT_SESS_CREATE_TS_BIT) - 1;
  static const int64_t CLIENT_SESS_ID_BIT = 32;
  static const int64_t CLIENT_SESS_ID_MASK = (1L << CLIENT_SESS_ID_BIT) - 1;

  ObTableLockOwnerID() :
    type_(static_cast<unsigned char>(ObLockOwnerType::INVALID_OWNER_TYPE)),
    id_(INVALID_ID) {}
  ObTableLockOwnerID(const ObTableLockOwnerID &other) :
    type_(other.type_), id_(other.id_)
  { hash_value_ = inner_hash(); }
  ObTableLockOwnerID(unsigned char type, int64_t id) :
    type_(type), id_(id)
  { hash_value_ = inner_hash(); }
  ~ObTableLockOwnerID() { reset(); }
public:
  int get_ddl_owner_id(int64_t &id) const;
  int64_t id() const { return id_; }
  unsigned char type() const { return type_; }
  bool is_session_id_owner() const
  { return type_ == static_cast<unsigned char>(ObLockOwnerType::SESS_ID_OWNER_TYPE); }
  bool is_default() const
  { return 0 == type_ && 0 == id_; }
  void reset()
  {
    type_ = static_cast<unsigned char>(ObLockOwnerType::INVALID_OWNER_TYPE);
    id_ = INVALID_ID;
  }
  bool is_valid() const
  {
    return (INVALID_ID != id_ &&
            is_lock_owner_type_valid(static_cast<ObLockOwnerType>(type_)));
  }
  static ObTableLockOwnerID default_owner();
  static ObTableLockOwnerID get_owner(const unsigned char type,
                                      const int64_t id);
  void set_default()
  { type_ = 0; id_ = 0; hash_value_ = inner_hash(); }
  // check valid.
  void convert_from_value_ignore_ret(const unsigned char owner_type,
                                     const int64_t id);
  int convert_from_value(const ObLockOwnerType owner_type,
                         const int64_t id);
  int convert_from_client_sessid(const uint32_t client_sessid,
                                 const uint64_t client_sess_create_ts);
  int convert_to_sessid(uint32_t &sessid) const;
  // assignment
  ObTableLockOwnerID &operator=(const ObTableLockOwnerID &other)
  {
    type_ = other.type_; id_ = other.id_;
    hash_value_ = inner_hash();
    return *this;
  }

  // compare operator
  bool operator == (const ObTableLockOwnerID &other) const
  { return type_ == other.type_ && id_ == other.id_; }
  bool operator >  (const ObTableLockOwnerID &other) const
  {
    return (type_ > other.type_
            || (type_ == other.type_ && id_ > other.id_));
  }
  bool operator != (const ObTableLockOwnerID &other) const
  { return type_ != other.type_ || id_ != other.id_; }
  bool operator <  (const ObTableLockOwnerID &other) const
  {
    return (type_ < other.type_
            || (type_ == other.type_ && id_ < other.id_));
  }
  bool operator <= (const ObTableLockOwnerID &other) const
  {
    return (type_ <= other.type_
            || (type_ == other.type_ && id_ <= other.id_));
  }
  bool operator >= (const ObTableLockOwnerID &other) const
  {
    return (type_ >= other.type_
            || (type_ == other.type_ && id_ >= other.id_));
  }
  int compare(const ObTableLockOwnerID &other) const
  {
    if (type_ == other.type_ && id_ == other.id_) {
      return 0;
    } else if (type_ < other.type_
               || (type_ == other.type_ && id_ < other.id_)) {
      return -1;
    } else {
      return 1;
    }
  }

  uint64_t hash() const
  { return hash_value_; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  uint64_t inner_hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&type_, sizeof(type_), hash_val);
    hash_val = murmurhash(&id_, sizeof(id_), hash_val);
    return hash_val;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV("type_name", get_name(static_cast<ObLockOwnerType>(type_)), K_(id), K_(hash_value));

private:
  int get_data_version_(uint64_t &data_version) const;
private:
  unsigned char type_;
  int64_t id_;
  uint64_t hash_value_;
};

class ObOldLockOwner
{
  friend class ObTableLockOwnerID;
public:
  static const int64_t INVALID_ID = -1;
  ObOldLockOwner() : pack_(INVALID_ID) {}
  ObOldLockOwner(const ObTableLockOwnerID &owner_id)
  {
    pack_ = 0;
    id_ = owner_id.id();
    type_ = owner_id.type();
  }
  ~ObOldLockOwner() { pack_ = INVALID_ID; }
public:
  int64_t raw_value() const { return pack_; }
  // without check whether it is valid.
  int convert_from_value(const int64_t packed_id);
  int64_t id() const { return id_; }
  int64_t type() const { return type_; }

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
  TO_STRING_KV(K_(lock_id), K_(lock_mode),
               "lock_mode_name", get_name(lock_mode_),
               K_(owner_id), K_(create_trans_id), K_(op_type),
               "op_type_name", get_name(op_type_),
               K_(lock_op_status),
               "lock_op_status_name", get_name(lock_op_status_),
               K_(lock_seq_no),
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

struct ObTableLockPrioOp
{
  OB_UNIS_VERSION(1);
public:
  ObTableLockPrioOp()
    : lock_op_(), priority_(ObTableLockPriority::INVALID)
  {}
  ObTableLockPrioOp(const ObTableLockPriority priority, const ObTableLockOp &lock_op)
    : lock_op_(lock_op), priority_(priority)
  {}
  bool is_valid() const
  { return ObTableLockPriority::INVALID != priority_ && lock_op_.is_valid(); }
public:
  TO_STRING_KV(K_(lock_op), K_(priority),
               "priority_name", get_name(priority_));
public:
  ObTableLockOp lock_op_;
  ObTableLockPriority priority_;
};

typedef common::ObSEArray<ObTableLockPrioOp, 10, TransModulePageAllocator> ObTableLockPrioOpArray;

struct ObTableLockPrioArg
{
  OB_UNIS_VERSION(1);
public:
  ObTableLockPrioArg()
    : priority_(ObTableLockPriority::INVALID) {}
  explicit ObTableLockPrioArg(const ObTableLockPriority &priority)
    : priority_(priority) {}
  bool is_valid() const { return ObTableLockPriority::INVALID != priority_; }
public:
  TO_STRING_KV(K_(priority),
               "priority_name", get_name(priority_));
public:
  ObTableLockPriority priority_;
};

struct ObObjLockPriorityTaskID
{
  explicit ObObjLockPriorityTaskID(
      const int64_t trans_id_value, const ObTableLockOwnerID &owner_id)
    : trans_id_value_(trans_id_value), owner_id_(owner_id) {}
  bool is_valid() const { return trans_id_value_ > 0 && owner_id_.is_valid(); }
  TO_STRING_KV(K_(trans_id_value), K_(owner_id));
  int64_t trans_id_value_;
  ObTableLockOwnerID owner_id_;
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
// Is used to store and traverse all priority op
typedef common::ObSimpleIterator<ObTableLockPrioOp, ObSimpleIteratorModIds::OB_OBJ_LOCK, 16> ObPrioOpIterator;

// the threshold of timeout interval which will enable the deadlock avoid.
static const int64_t MIN_DEADLOCK_AVOID_TIMEOUT_US = 60 * 1000 * 1000; // 1 min
bool is_deadlock_avoid_enabled(const bool is_from_sql, const int64_t expire_time);

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_COMMON_ */
