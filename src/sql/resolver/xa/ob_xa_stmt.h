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

#ifndef _OB_XA_STMT_H_
#define _OB_XA_STMT_H_

#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ob_cmd.h"

namespace oceanbase
{
namespace sql
{
class ObXaStmt : public ObStmt, public ObICmd
{
public:
  explicit ObXaStmt(const stmt::StmtType stmt_type)
    : ObStmt(stmt_type),
      xid_string_(),
      gtrid_string_(),
      bqual_string_(),
      format_id_(-1),
      flags_(0)
  {
  }
  ~ObXaStmt()
  {
  }

  void set_xa_string(common::ObString& gtrid_string)
  {
    gtrid_string_ = gtrid_string;
    if (gtrid_string.length() <= MAX_XID_LENGTH / 2) {
      xid_string_.reset();
      (void)xid_string_.assign_buffer(xid_buffer_, sizeof(xid_buffer_));
      xid_string_.write(gtrid_string.ptr(), gtrid_string.length());
    }
  }

  void set_xa_string(common::ObString &gtrid_string, common::ObString &bqual_string)
  {
    gtrid_string_ = gtrid_string;
    bqual_string_ = bqual_string;
    if ((gtrid_string.length() <= MAX_XID_LENGTH / 2)
        && (bqual_string.length() <= MAX_XID_LENGTH / 2)) {
      xid_string_.reset();
      (void)xid_string_.assign_buffer(xid_buffer_, sizeof(xid_buffer_));
      xid_string_.write(gtrid_string.ptr(), gtrid_string.length());
      xid_string_.write(bqual_string.ptr(), bqual_string.length());
    }
  }

  common::ObString get_xa_string() {
    return xid_string_;
  } 

  common::ObString get_gtrid_string() const
  {
    return gtrid_string_;
  }

  common::ObString get_bqual_string() const
  {
    return bqual_string_;
  }

  void set_format_id(const int64_t format_id)
  {
    format_id_ = format_id;
  }

  int64_t get_format_id() const
  {
    return format_id_;
  }

  int get_cmd_type() const {
    return stmt_type_;
  }

  void set_flags(const int64_t flags)
  {
    flags_ = flags;
  }

  int64_t get_flags() const
  {
    return flags_;
  }

  bool is_valid_oracle_xid()
  {
    return 0 <= format_id_
           && 0 < gtrid_string_.length()
           && MAX_GTRID_LENGTH >= gtrid_string_.length()
           && 0 < bqual_string_.length()
           && MAX_BQUAL_LENGTH >= bqual_string_.length();
  }

  static bool is_valid_oracle_xid(const int64_t format_id,
                                  const common::ObString &gtrid_string,
                                  const common::ObString &bqual_string)
  {
    return 0 <= format_id
           && 0 < gtrid_string.length()
           && MAX_GTRID_LENGTH >= gtrid_string.length()
           && 0 < bqual_string.length()
           && MAX_BQUAL_LENGTH >= bqual_string.length();
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(xid_string), K_(gtrid_string),
               K_(bqual_string), K_(format_id), K_(flags));
public:
  static const int32_t MAX_XID_LENGTH = 128;
  static const int32_t MAX_GTRID_LENGTH = 64;
  static const int32_t MAX_BQUAL_LENGTH = 64;

private:
  DISALLOW_COPY_AND_ASSIGN(ObXaStmt);
private:
  char xid_buffer_[MAX_XID_LENGTH];
  common::ObString xid_string_;
  common::ObString gtrid_string_;
  common::ObString bqual_string_;
  int64_t format_id_;
  int64_t flags_;
};

class ObXaStartStmt : public ObXaStmt
{
public:
  explicit ObXaStartStmt()
      : ObXaStmt(stmt::T_XA_START)
  {
  }
};

class ObXaEndStmt : public ObXaStmt
{
public:
  explicit ObXaEndStmt()
      : ObXaStmt(stmt::T_XA_END)
  {
  }
};

class ObXaPrepareStmt : public ObXaStmt
{
public:
  explicit ObXaPrepareStmt()
      : ObXaStmt(stmt::T_XA_PREPARE)
  {
  }
};

class ObXaCommitStmt : public ObXaStmt
{
public:
  explicit ObXaCommitStmt()
      : ObXaStmt(stmt::T_XA_COMMIT)
  {
  }
};

class ObXaRollBackStmt : public ObXaStmt
{
public:
  explicit ObXaRollBackStmt()
      : ObXaStmt(stmt::T_XA_ROLLBACK)
  {
  }
};

} // end sql
} // end oceanbase

#endif
