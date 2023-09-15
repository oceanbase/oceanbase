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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_STRUCT_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_STRUCT_H_

#include "lib/string/ob_sql_string.h"
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/ob_inner_kv_table_operator.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_compatible.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{

// Initial round id for new round.
const int64_t OB_START_LOG_ARCHIVE_ROUND_ID = 0;

// archive state machine
struct ObArchiveRoundState
{
  enum class Status : int64_t
  {
    INVALID = 0,
    PREPARE,
    BEGINNING,
    DOING,
    INTERRUPTED,
    STOPPING,
    STOP,
    SUSPENDING,
    SUSPEND,
    MAX_STATUS
  };

  Status status_;

  ObArchiveRoundState() : status_(Status::INVALID) {}
  ObArchiveRoundState(const ObArchiveRoundState &other) : status_(other.status_) {}
  bool operator==(const ObArchiveRoundState &other) const
  {
    return status_ == other.status_;
  }

  bool operator!=(const ObArchiveRoundState &other) const
  {
    return !(*this == other);
  }

  void operator=(const ObArchiveRoundState &other)
  {
    status_ = other.status_;
  }


  #define PROPERTY_DECLARE_STATUS(status_alias, status) \
  bool is_##status_alias() const \
  { \
    return status_ == status; \
  } \
  \
  void set_##status_alias() \
  { status_ = status; } \
  \
  static ObArchiveRoundState status_alias() \
  { \
    ObArchiveRoundState s; \
    s.status_ = status; \
    return s; \
  }

  PROPERTY_DECLARE_STATUS(invalid, Status::INVALID);
  PROPERTY_DECLARE_STATUS(prepare, Status::PREPARE);
  PROPERTY_DECLARE_STATUS(beginning, Status::BEGINNING);
  PROPERTY_DECLARE_STATUS(doing, Status::DOING);
  PROPERTY_DECLARE_STATUS(stopping, Status::STOPPING);
  PROPERTY_DECLARE_STATUS(stop, Status::STOP);
  PROPERTY_DECLARE_STATUS(interrupted, Status::INTERRUPTED);
  PROPERTY_DECLARE_STATUS(suspending, Status::SUSPENDING);
  PROPERTY_DECLARE_STATUS(suspend, Status::SUSPEND);

#undef PROPERTY_DECLARE_STATUS

  bool is_valid() const;
  const char *to_status_str() const;
  int set_status(const char *status_str);

  TO_STRING_KV("status", to_status_str());
};

// current round
struct ObTenantArchiveHisRoundAttr;
struct ObTenantArchivePieceAttr;
// Define dest round table row structure.
struct ObTenantArchiveRoundAttr final : public ObIInnerTableRow
{
  // Define dest round key.
  struct Key final : public ObIInnerTableKey
  {
    uint64_t tenant_id_; // user tenant id
    int64_t dest_no_; // archive dest channel no.

    Key()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      dest_no_ = -1;
    }

    Key(const uint64_t tenant_id, int64_t dest_no)
      : tenant_id_(tenant_id), dest_no_(dest_no)
    {
    }

    Key(const Key &other)
      : tenant_id_(other.tenant_id_), dest_no_(other.dest_no_) {}

    void operator=(const Key &other)
    {
      tenant_id_ = other.tenant_id_;
      dest_no_ = other.dest_no_;
    }

    bool operator==(const Key &other) const
    {
      return tenant_id_ == other.tenant_id_ && dest_no_ == other.dest_no_;
    }

    bool operator!=(const Key &other) const
    {
      return !(*this == other);
    }

    // Return if primary key valid.
    bool is_pkey_valid() const override;

    // Fill primary key to dml.
    int fill_pkey_dml(ObDMLSqlSplicer &dml) const override;

    TO_STRING_KV(K_(tenant_id), K_(dest_no));
  };

  Key key_;
  int64_t incarnation_;
  int64_t dest_id_; // archive dest identification.
  int64_t round_id_;
  ObArchiveRoundState state_;
  SCN start_scn_;
  SCN checkpoint_scn_;
  SCN max_scn_;
  ObArchiveCompatible compatible_;

  int64_t base_piece_id_;
  int64_t used_piece_id_;
  int64_t piece_switch_interval_; // unit: us

  int64_t frozen_input_bytes_;
  int64_t frozen_output_bytes_;
  int64_t active_input_bytes_;
  int64_t active_output_bytes_;
  int64_t deleted_input_bytes_;
  int64_t deleted_output_bytes_;

  ObBackupPathString path_;
  ObBackupDefaultFixedLenString comment_;

  ObTenantArchiveRoundAttr()
  {
    incarnation_ = OB_START_INCARNATION;
    dest_id_ = 0;
    round_id_ = 0;
    base_piece_id_ = 0;
    used_piece_id_ = 0;
    piece_switch_interval_ = 0;
    start_scn_ = share::SCN::min_scn();
    checkpoint_scn_ = share::SCN::min_scn();
    max_scn_ = share::SCN::min_scn();
    frozen_input_bytes_ = 0;
    frozen_output_bytes_ = 0;
    active_input_bytes_ = 0;
    active_output_bytes_ = 0;
    deleted_input_bytes_ = 0;
    deleted_output_bytes_ = 0;
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;

  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(ObDMLSqlSplicer &dml) const override;



  OB_INLINE int set_path(const char *path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_path(const ObBackupPathString &path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_comment(const char *comment)
  {
    return comment_.assign(comment);
  }

  OB_INLINE int set_compatible_version(const int64_t version)
  {
    return compatible_.set_version(version);
  }

  OB_INLINE int set_status(const char *status_str)
  {
    return state_.set_status(status_str);
  }

  OB_INLINE void switch_state_to(const ObArchiveRoundState &status)
  {
    state_ = status;
  }

  void set_stop();
  int deep_copy_from(const ObTenantArchiveRoundAttr &other);
  // Generate next round content from current round, with archive state 'PREPARE'.
  int generate_next_round(const int64_t incarnation, const int64_t dest_id,
      const int64_t piece_switch_interval, const ObBackupPathString &path,
      ObTenantArchiveRoundAttr &next_round) const;
  // Generate first piece of the round with status 'ACTIVE' and file_status 'INCOMPLETE'
  int generate_first_piece(ObTenantArchivePieceAttr &first_piece) const;
  ObTenantArchiveHisRoundAttr generate_his_round() const;
  // Generate initial round, and set status to 'PREPARE'.
  static int generate_initial_round(const Key &key, const int64_t incarnation,
      const int64_t dest_id, const int64_t piece_switch_interval, const ObBackupPathString &path,
      ObTenantArchiveRoundAttr &initial_round);

  TO_STRING_KV(K_(key), K_(incarnation), K_(dest_id), K_(round_id), K_(state), K_(start_scn),
    K_(checkpoint_scn), K_(max_scn), K_(compatible), K_(base_piece_id), K_(used_piece_id), K_(piece_switch_interval),
    K_(frozen_input_bytes), K_(frozen_output_bytes), K_(active_input_bytes), K_(active_output_bytes),
    K_(deleted_input_bytes), K_(deleted_output_bytes), K_(path), K_(comment));
};


// Define history dest round table row structure.
struct ObTenantArchiveHisRoundAttr final : public ObIInnerTableRow
{
  // Define his dest round key.
  struct Key final : public ObIInnerTableKey
  {
    uint64_t tenant_id_; // user tenant id
    int64_t dest_no_; // archive dest channel no.
    int64_t round_id_;

    Key()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      dest_no_ = -1;
      round_id_ = 0;
    }

    Key(const uint64_t tenant_id, const int64_t dest_no, const int64_t round_id)
      : tenant_id_(tenant_id), dest_no_(dest_no), round_id_(round_id)
    {
    }

    Key(const Key &other)
      : tenant_id_(other.tenant_id_), dest_no_(other.dest_no_), round_id_(other.round_id_) {}

    void operator=(const Key &other)
    {
      tenant_id_ = other.tenant_id_;
      dest_no_ = other.dest_no_;
      round_id_ = other.round_id_;
    }

    bool operator==(const Key &other) const
    {
      return tenant_id_ == other.tenant_id_ && dest_no_ == other.dest_no_ && round_id_ == other.round_id_;
    }

    bool operator!=(const Key &other) const
    {
      return !(*this == other);
    }

    // Return if primary key valid.
    bool is_pkey_valid() const override;

    // Fill primary key to dml.
    int fill_pkey_dml(ObDMLSqlSplicer &dml) const override;

    TO_STRING_KV(K_(tenant_id), K_(dest_no), K_(round_id));
  };

  Key key_;
  int64_t incarnation_;
  int64_t dest_id_;
  SCN start_scn_;
  SCN checkpoint_scn_;
  SCN max_scn_;
  ObArchiveCompatible compatible_;

  int64_t base_piece_id_;
  int64_t used_piece_id_;
  int64_t piece_switch_interval_;

  int64_t input_bytes_;
  int64_t output_bytes_;
  int64_t deleted_input_bytes_;
  int64_t deleted_output_bytes_;

  ObBackupPathString path_;
  ObBackupDefaultFixedLenString comment_;

  ObTenantArchiveHisRoundAttr()
  {
    incarnation_ = OB_START_INCARNATION;
    dest_id_ = 0;
    base_piece_id_ = 0;
    used_piece_id_ = 0;
    piece_switch_interval_ = 0;
    start_scn_ = share::SCN::min_scn();
    checkpoint_scn_ = share::SCN::min_scn();
    max_scn_ = share::SCN::min_scn();
    input_bytes_ = 0;
    output_bytes_ = 0;
    deleted_input_bytes_ = 0;
    deleted_output_bytes_ = 0;
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;

  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(ObDMLSqlSplicer &dml) const override;


  OB_INLINE int set_path(const ObBackupPathString &path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_path(const char *path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_comment(const char *comment)
  {
    return comment_.assign(comment);
  }

  OB_INLINE int set_compatible_version(const int64_t version)
  {
    return compatible_.set_version(version);
  }

  ObTenantArchiveRoundAttr generate_round() const;

  TO_STRING_KV(K_(key), K_(incarnation), K_(dest_id), K_(start_scn), K_(checkpoint_scn),
    K_(max_scn), K_(compatible), K_(base_piece_id), K_(used_piece_id), K_(piece_switch_interval),
    K_(input_bytes), K_(output_bytes), K_(deleted_input_bytes), K_(deleted_output_bytes),
    K_(path), K_(comment));
};


// archive piece
struct ObArchivePieceStatus final
{
  OB_UNIS_VERSION(1);
public:
  enum class Status : int64_t
  {
    INACTIVE = 0,
    ACTIVE = 1,
    FROZEN = 2,
    MAX_STATUS
  };

  Status status_;

  ObArchivePieceStatus() : status_(Status::INACTIVE) {}
  ObArchivePieceStatus(const ObArchivePieceStatus &other) : status_(other.status_) {}
  bool operator==(const ObArchivePieceStatus &other) const
  {
    return status_ == other.status_;
  }

  bool operator!=(const ObArchivePieceStatus &other) const
  {
    return !(*this == other);
  }

  void operator=(const ObArchivePieceStatus &other)
  {
    status_ = other.status_;
  }

  #define PROPERTY_DECLARE_STATUS(status_alias, status) \
  bool is_##status_alias() const \
  { \
    return status_ == status; \
  } \
  \
  void set_##status_alias() \
  { status_ = status; } \
  \
  static ObArchivePieceStatus status_alias() \
  { \
    ObArchivePieceStatus s; \
    s.status_ = status; \
    return s; \
  }

  PROPERTY_DECLARE_STATUS(inactive, Status::INACTIVE);
  PROPERTY_DECLARE_STATUS(active, Status::ACTIVE);
  PROPERTY_DECLARE_STATUS(frozen, Status::FROZEN);

#undef PROPERTY_DECLARE_STATUS

  bool is_valid() const;
  const char *to_status_str() const;
  int set_status(const char *status_str);

  TO_STRING_KV("status", to_status_str());
};


struct ObPieceKey final
{
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;

  ObPieceKey()
  {
    dest_id_ = 0;
    round_id_ = 0;
    piece_id_ = 0;
  }

  ObPieceKey(const int64_t dest_id, const int64_t round_id, const int64_t piece_id)
    : dest_id_(dest_id), round_id_(round_id), piece_id_(piece_id)
  {
  }

  ObPieceKey(const ObPieceKey &other)
    : dest_id_(other.dest_id_), round_id_(other.round_id_),
      piece_id_(other.piece_id_) {}

  void operator=(const ObPieceKey &other)
  {
    dest_id_ = other.dest_id_;
    round_id_ = other.round_id_;
    piece_id_ = other.piece_id_;
  }

  bool operator==(const ObPieceKey &other) const
  {
    return dest_id_ == other.dest_id_
          && round_id_ == other.round_id_
          && piece_id_ == other.piece_id_;
  }

  bool operator!=(const ObPieceKey &other) const
  {
    return !(*this == other);
  }

  bool operator<(const ObPieceKey &other) const {
    bool ret = false;
    if (round_id_ < other.round_id_) {
      ret = true;
    } else if (round_id_ == other.round_id_ && piece_id_ < other.piece_id_) {
      ret = true;
    }
    return ret;
  }

  TO_STRING_KV(K_(dest_id), K_(round_id), K_(piece_id));
};


// Define piece files table row structure.
struct ObTenantArchivePieceAttr final : public ObIInnerTableRow
{
  // Define piece key.
  struct Key final : public ObIInnerTableKey
  {
    OB_UNIS_VERSION(1);
  public:
    uint64_t tenant_id_; // user tenant id
    int64_t dest_id_; // archive dest id.
    int64_t round_id_;
    int64_t piece_id_;

    Key()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      dest_id_ = 0;
      round_id_ = 0;
      piece_id_ = 0;
    }

    Key(const uint64_t tenant_id, const int64_t dest_id, const int64_t round_id, const int64_t piece_id)
      : tenant_id_(tenant_id), dest_id_(dest_id), round_id_(round_id), piece_id_(piece_id)
    {
    }

    Key(const Key &other)
      : tenant_id_(other.tenant_id_), dest_id_(other.dest_id_), round_id_(other.round_id_),
        piece_id_(other.piece_id_) {}

    void operator=(const Key &other)
    {
      tenant_id_ = other.tenant_id_;
      dest_id_ = other.dest_id_;
      round_id_ = other.round_id_;
      piece_id_ = other.piece_id_;
    }

    bool operator==(const Key &other) const
    {
      return tenant_id_ == other.tenant_id_ && dest_id_ == other.dest_id_
            && round_id_ == other.round_id_ && piece_id_ == other.piece_id_;
    }

    bool operator!=(const Key &other) const
    {
      return !(*this == other);
    }

    // Return if primary key valid.
    bool is_pkey_valid() const override;

    // Fill primary key to dml.
    int fill_pkey_dml(ObDMLSqlSplicer &dml) const override;

    TO_STRING_KV(K_(tenant_id), K_(dest_id), K_(round_id), K_(piece_id));
  };

  OB_UNIS_VERSION(1);
public:
  Key key_;
  int64_t incarnation_;
  int64_t dest_no_;
  int64_t file_count_;
  SCN start_scn_;
  SCN checkpoint_scn_;
  SCN max_scn_;
  SCN end_scn_;
  ObArchiveCompatible compatible_;

  int64_t input_bytes_;
  int64_t output_bytes_;

  ObArchivePieceStatus status_;
  ObBackupFileStatus::STATUS file_status_;

  int64_t cp_file_id_;
  int64_t cp_file_offset_;
  ObBackupPathString path_;

  ObTenantArchivePieceAttr()
  {
    incarnation_ = OB_START_INCARNATION;
    dest_no_ = -1;
    file_count_ = 0;
    input_bytes_ = 0;
    output_bytes_ = 0;
    cp_file_id_ = 0;
    cp_file_offset_ = 0;
    start_scn_ = share::SCN::min_scn();
    checkpoint_scn_ = share::SCN::min_scn();
    max_scn_ = share::SCN::min_scn();
    end_scn_ = share::SCN::min_scn();
  }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;

  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(ObDMLSqlSplicer &dml) const override;

  int assign(const ObTenantArchivePieceAttr &other);

  OB_INLINE int set_path(const ObBackupPathString &path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_path(const char *path)
  {
    return path_.assign(path);
  }

  OB_INLINE int set_compatible_version(const int64_t version)
  {
    return compatible_.set_version(version);
  }

  OB_INLINE int set_status(const char *status_str)
  {
    return status_.set_status(status_str);
  }

  OB_INLINE void set_active()
  {
    status_.set_active();
  }

  OB_INLINE void set_frozen()
  {
    status_.set_frozen();
  }

  OB_INLINE bool is_active() const
  {
    return status_.is_active();
  }

  OB_INLINE bool is_frozen() const
  {
    return status_.is_frozen();
  }

  TO_STRING_KV(K_(key), K_(incarnation), K_(dest_no), K_(file_count),
    K_(start_scn), K_(checkpoint_scn), K_(max_scn), K_(end_scn), K_(compatible), K_(input_bytes),
    K_(output_bytes), K_(status), K_(file_status), K_(cp_file_id), K_(cp_file_offset), K_(path));
};


// Define ls progress table row structure.
struct ObLSArchivePersistInfo final : public ObIInnerTableRow
{
  // Define ls piece key.
  struct Key final : public ObIInnerTableKey
  {
    uint64_t tenant_id_; // user tenant id
    int64_t dest_id_; // archive dest id.
    int64_t round_id_;
    int64_t piece_id_;
    ObLSID ls_id_;

    Key()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      dest_id_ = 0;
      round_id_ = 0;
      piece_id_ = 0;
    }

    Key(const uint64_t tenant_id, const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const ObLSID &ls_id)
      : tenant_id_(tenant_id), dest_id_(dest_id), round_id_(round_id), piece_id_(piece_id), ls_id_(ls_id)
    {
    }

    Key(const Key &other)
      : tenant_id_(other.tenant_id_), dest_id_(other.dest_id_), round_id_(other.round_id_),
        piece_id_(other.piece_id_), ls_id_(other.ls_id_) {}

    void operator=(const Key &other)
    {
      tenant_id_ = other.tenant_id_;
      dest_id_ = other.dest_id_;
      round_id_ = other.round_id_;
      piece_id_ = other.piece_id_;
      ls_id_ = other.ls_id_;
    }

    bool operator==(const Key &other) const
    {
      return tenant_id_ == other.tenant_id_ && dest_id_ == other.dest_id_
            && round_id_ == other.round_id_ && piece_id_ == other.piece_id_
            && ls_id_ == other.ls_id_;
    }

    bool operator!=(const Key &other) const
    {
      return !(*this == other);
    }

    // Return if primary key valid.
    bool is_pkey_valid() const override;

    // Fill primary key to dml.
    int fill_pkey_dml(ObDMLSqlSplicer &dml) const override;

    void reset();

    TO_STRING_KV(K_(tenant_id), K_(dest_id), K_(round_id), K_(piece_id), K_(ls_id));
  };

  Key key_;
  int64_t incarnation_;
  SCN start_scn_;        // piece start ts
  uint64_t start_lsn_;                // piece start lsn
  SCN checkpoint_scn_;
  uint64_t lsn_;
  int64_t archive_file_id_;
  int64_t archive_file_offset_;
  int64_t input_bytes_;
  int64_t output_bytes_;
  ObArchiveRoundState state_;

  ObLSArchivePersistInfo() { reset(); }
  ~ObLSArchivePersistInfo() { reset(); }

  // Return if primary key valid.
  bool is_pkey_valid() const override
  {
    return key_.is_pkey_valid();
  }

  // Fill primary key to dml.
  int fill_pkey_dml(ObDMLSqlSplicer &dml) const override
  {
    return key_.fill_pkey_dml(dml);
  }

  // Return if both primary key and value are valid.
  bool is_valid() const override;

  // Parse row from the sql result, the result has full columns.
  int parse_from(common::sqlclient::ObMySQLResult &result) override;

  // Fill primary key and value to dml.
  int fill_dml(ObDMLSqlSplicer &dml) const override;


  OB_INLINE int set_status(const char *status_str)
  {
    return state_.set_status(status_str);
  }

  int set_stop(const uint64_t tenant_id,
      const int64_t incarnation,
      const int64_t round,
      const int64_t dest_id);
  void reset();
  void refresh_state(const ObArchiveRoundState &server_state);
  // "pk1, pk2, c3, c4"
  int build_column_names(ObSqlString &column_names);

  TO_STRING_KV(K_(key), K_(incarnation), K_(start_scn), K_(start_lsn), K_(checkpoint_scn), K_(lsn),
    K_(archive_file_id), K_(archive_file_offset), K_(input_bytes), K_(output_bytes), K_(state));
};



// define memory structure
// log stream
struct ObArchiveLSPieceSummary
{
  uint64_t tenant_id_;
  ObLSID ls_id_;
  bool is_archiving_;
  bool is_deleted_;

  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  int64_t incarnation_;
  ObArchiveRoundState state_;

  SCN start_scn_;
  SCN checkpoint_scn_;
  uint64_t min_lsn_;
  uint64_t max_lsn_;
  int64_t input_bytes_;
  int64_t output_bytes_;

  ObArchiveLSPieceSummary();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(is_archiving), K_(is_deleted), K_(dest_id),
    K_(round_id), K_(piece_id), K_(incarnation), K_(state), K_(start_scn),
    K_(checkpoint_scn), K_(min_lsn), K_(max_lsn), K_(input_bytes), K_(output_bytes));
};


struct ObLSDestRoundSummary
{
  struct OnePiece
  {
    int64_t piece_id_;
    SCN start_scn_;
    SCN checkpoint_scn_;
    uint64_t min_lsn_;
    uint64_t max_lsn_;
    int64_t input_bytes_;
    int64_t output_bytes_;

    TO_STRING_KV(K_(piece_id), K_(start_scn), K_(checkpoint_scn), K_(min_lsn), K_(max_lsn), K_(input_bytes), K_(output_bytes));
  };

  uint64_t tenant_id_;
  int64_t dest_id_;
  // If is_archiving_=false, then set round_id 0.
  int64_t round_id_;
  ObLSID ls_id_;
  bool is_deleted_; // mark deleted ls.
  ObArchiveRoundState state_;
  SCN start_scn_;
  SCN checkpoint_scn_;
  // Ordered by pieceid.
  common::ObArray<OnePiece> piece_list_;

  ObLSDestRoundSummary();
  bool has_piece() const;
  int add_one_piece(const ObArchiveLSPieceSummary &piece);
  void reset();
  bool is_valid() const;

  // Get piece position in 'pieces_array_', return -1 if not exist.
  int64_t get_piece_idx(const int64_t piece_id) const;
  int64_t min_piece_id() const;
  int64_t max_piece_id() const;
  // Return true if ls is deleted and the piece_id is the biggest piece id.
  int check_is_last_piece_for_deleted_ls(const int64_t piece_id, bool &last_piece) const;

  TO_STRING_KV(K_(tenant_id), K_(dest_id), K_(round_id), K_(ls_id), K_(is_deleted), K_(state),
    K_(start_scn), K_(checkpoint_scn), K_(piece_list));
};


struct ObDestRoundSummary
{
  uint64_t tenant_id_;
  int64_t dest_id_;
  int64_t round_id_;

  // Ordered by pieceid.
  common::ObArray<ObLSDestRoundSummary> ls_round_list_;

  int64_t ls_count() const;
  bool is_valid() const;
  int add_ls_dest_round_summary(const ObLSDestRoundSummary &dest_round_summary);

  TO_STRING_KV(K_(tenant_id), K_(dest_id), K_(round_id), K_(ls_round_list));
};

struct ObArchiveLSMetaType final
{
  // NB: the enum type is not only the type of ls meta, but also will be its subdir,
  // so pay attention to give the type an appropriate name while the length of its name
  // 'SMALLER' than then MAX_TYPE_LEN
  static const int64_t MAX_TYPE_LEN = 100;
  enum Type : int64_t
  {
    INVALID_TYPE = 0,
    // add task type here
    SCHEMA_META = 1,
    MAX_TYPE,
  };

  Type type_;
  ObArchiveLSMetaType() : type_(Type::INVALID_TYPE) {}
  explicit ObArchiveLSMetaType(const Type type) : type_(type) {}

  bool is_valid() const;
  uint64_t hash() const { return static_cast<uint64_t>(type_); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  int compare(const ObArchiveLSMetaType &other) const;
  bool operator==(const ObArchiveLSMetaType &other) const { return 0 == compare(other); }
  bool operator!=(const ObArchiveLSMetaType &other) const { return !operator==(other); }
  const char *get_type_str() const;
  int get_next_type();

  TO_STRING_KV("type", get_type_str());
};
}
}

#endif
