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

#ifndef OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_
#define OCEANBASE_TRANSACTION_OB_MULTI_DATA_SOURCE_

#include "share/ob_cluster_version.h"
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_list.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/string/ob_string.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/buffer_ctx.h"

namespace oceanbase
{

namespace share
{
class ObLSID;
}

namespace memtable
{
class ObMemtableCtx;
}

namespace transaction
{
class ObPartTransCtx;

struct ObMulSourceDataNotifyArg;

enum class ObTxDataSourceType : int64_t
{
  UNKNOWN = -1,
  // for memory table
  MEM_TABLE = 0,
  // for table lock
  TABLE_LOCK = 1,
  // for log stream table(create log stream)
  LS_TABLE = 2,
  // for liboblog
  DDL_BARRIER = 5,
  // for all ddl trans(record incremental schema)
  DDL_TRANS = 6,
  // for standby upgrade
  STANDBY_UPGRADE = 8,
  BEFORE_VERSION_4_1 = 13,
#define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(helper_class_name, buffer_ctx_type, ID, ENUM_NAME) ENUM_NAME = ID,
  #include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
#undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
  MAX_TYPE = 100
};

static const char * to_str_mds_type(const ObTxDataSourceType & mds_type )
{
  const char * str = "INVALID";
  switch(mds_type)
  {
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, UNKNOWN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, MEM_TABLE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TABLE_LOCK);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, LS_TABLE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DDL_BARRIER);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DDL_TRANS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, STANDBY_UPGRADE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, BEFORE_VERSION_4_1);

    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST1);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST2);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST3);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, CREATE_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DELETE_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, UNBIND_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_IN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, FINISH_TRANSFER_OUT);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, FINISH_TRANSFER_IN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_TASK);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT_PREPARE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT_V2);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_MOVE_TX_CTX);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_DEST_PREPARE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, CHANGE_TABLET_TO_TABLE_MDS);

    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, MAX_TYPE);
  }
  return str;
}

enum class NotifyType : int64_t
{
  UNKNOWN = -1,
  REGISTER_SUCC = 0,
  ON_REDO = 1,
  TX_END = 2,
  ON_PREPARE = 3,
  ON_COMMIT = 4,
  ON_ABORT = 5
};

static const char * to_str_notify_type(const NotifyType & notify_type)
{
  const char * str = "INVALID";
  switch(notify_type)
  {
    TRX_ENUM_CASE_TO_STR(NotifyType, UNKNOWN);
    TRX_ENUM_CASE_TO_STR(NotifyType, REGISTER_SUCC);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_REDO);
    TRX_ENUM_CASE_TO_STR(NotifyType, TX_END);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_PREPARE);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_COMMIT);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_ABORT);
  }
  return str;
}

class ObTxBufferNode
{
  friend class ObPartTransCtx;
  friend class ObTxExecInfo;
  friend class ObMulSourceTxDataNotifier;
  friend class ObTxMDSCache;
  friend class ObTxBufferNodeWrapper;
  OB_UNIS_VERSION(1);

public:
  ObTxBufferNode() : type_(ObTxDataSourceType::UNKNOWN), data_() { reset(); }
  ~ObTxBufferNode() {}
  int init(const ObTxDataSourceType type,
           const common::ObString &data,
           const share::SCN &base_scn,
           storage::mds::BufferCtx *ctx);
  bool is_valid() const
  {
    bool valid_member = false;
    valid_member = type_ > ObTxDataSourceType::UNKNOWN && type_ < ObTxDataSourceType::MAX_TYPE
                   && data_.length() > 0;
    return valid_member;
  }
  void reset()
  {
    register_no_ = 0;
    type_ = ObTxDataSourceType::UNKNOWN;
    data_.reset();
    has_submitted_ = false;
    has_synced_ = false;
    mds_base_scn_.reset();
  }

  static bool is_valid_register_no(const int64_t register_no) { return register_no > 0; }
  int set_mds_register_no(const uint64_t register_no);
  uint64_t get_register_no() const { return register_no_; }

  // only for some mds types of CDC
  // can not be used by observer functions
  bool allow_to_use_mds_big_segment() const { return type_ == ObTxDataSourceType::DDL_TRANS; }

  void replace_data(const common::ObString &data);

  ObString &get_data() { return data_; }
  int64_t get_data_size() const { return data_.length(); }
  ObTxDataSourceType get_data_source_type() const { return type_; }
  const ObString &get_data_buf() const { return data_; }
  void *get_ptr() { return data_.ptr(); }

  void set_submitted() { has_submitted_ = true; };
  bool is_submitted() const { return has_submitted_; };

  void set_synced() { has_synced_ = true; }
  bool is_synced() const { return has_synced_; }

  const share::SCN &get_base_scn() { return mds_base_scn_; }

  bool operator==(const ObTxBufferNode &buffer_node) const;

  void log_sync_fail()
  {
    has_submitted_ = false;
    has_synced_ = false;
  }
  storage::mds::BufferCtxNode &get_buffer_ctx_node() const { return buffer_ctx_node_; }

  TO_STRING_KV(K(register_no_),
               K(has_submitted_),
               K(has_synced_),
               "type",
               to_str_mds_type(type_),
               K(data_.length()));

private:
  uint64_t register_no_;
  bool has_submitted_;
  bool has_synced_;
  share::SCN mds_base_scn_;
  ObTxDataSourceType type_;
  common::ObString data_;
  mutable storage::mds::BufferCtxNode buffer_ctx_node_;
};

typedef common::ObSEArray<ObTxBufferNode, 1> ObTxBufferNodeArray;
typedef common::ObSEArray<storage::mds::BufferCtxNode , 1> ObTxBufferCtxArray;

// manage mds_op contain (buffer_node, buffer, buffer_ctx)
class ObTxBufferNodeWrapper
{
  OB_UNIS_VERSION(1);
public:
  ObTxBufferNodeWrapper() : tx_id_(0), node_()
  {}
  ObTxBufferNodeWrapper(const ObTxBufferNodeWrapper &) = delete;
  ObTxBufferNodeWrapper &operator=(const ObTxBufferNodeWrapper &) = delete;
  ~ObTxBufferNodeWrapper();
  const ObTxBufferNode &get_node() const { return node_; }
  int64_t get_tx_id() const { return tx_id_; }
  int pre_alloc(int64_t tx_id, const ObTxBufferNode &node, ObIAllocator &allocator);
  // deep_copy by node
  int assign(int64_t tx_id, const ObTxBufferNode &node, ObIAllocator &allocator, bool has_pre_alloc);
  int assign(ObIAllocator &allocator, const ObTxBufferNodeWrapper &node_wrapper);

  TO_STRING_KV(K_(tx_id), K_(node));
private:
  int64_t tx_id_;
  ObTxBufferNode node_;
};

class ObMulSourceTxDataNotifier
{
public:
  static int notify(const ObTxBufferNodeArray &array,
                    const NotifyType type,
                    const ObMulSourceDataNotifyArg &arg,
                    ObPartTransCtx *part_ctx,
                    int64_t &total_time);
  static int notify_table_lock(const ObTxBufferNodeArray &array,
                               const ObMulSourceDataNotifyArg &arg,
                               ObPartTransCtx *part_ctx,
                               int64_t &total_time);
private:
  static void ob_abort_log_cb_notify_(const NotifyType type, int err_code, bool for_replay);
  static int notify_table_lock(const NotifyType type,
                               const char *buf, const int64_t len,
                               const ObMulSourceDataNotifyArg &arg,
                               memtable::ObMemtableCtx *mt_ctx);
  static int notify_ls_table(const NotifyType type,
                             const char *buf, const int64_t len,
                             const ObMulSourceDataNotifyArg &arg);
  static int notify_standby_upgrade(const NotifyType type,
                             const char *buf, const int64_t len,
                             const ObMulSourceDataNotifyArg &arg);
  static int notify_ddl_trans(const NotifyType type,
                              const char *buf, const int64_t len,
                              const ObMulSourceDataNotifyArg &arg);
  static int notify_ddl_barrier(const NotifyType type,
                                const char *buf, const int64_t len,
                                const ObMulSourceDataNotifyArg &arg);
};

class ObMulSourceTxDataDump
{
public:
   static const char* dump_buf(ObTxDataSourceType source_type, const char * buf,const int64_t len);
private:


};

struct ObRegisterMdsFlag
{
  bool need_flush_redo_instantly_;
  share::SCN mds_base_scn_;

  ObRegisterMdsFlag() { reset(); }
  void reset()
  {
    need_flush_redo_instantly_ = false;
    mds_base_scn_.reset();
  }

  TO_STRING_KV(K(need_flush_redo_instantly_), K(mds_base_scn_));

  OB_UNIS_VERSION(1);
};

class ObMDSInnerSQLStr
{
  OB_UNIS_VERSION(1);

public:
  ObMDSInnerSQLStr();
  ~ObMDSInnerSQLStr();
  void reset();

  int set(const char *mds_buf,
          const int64_t mds_buf_len,
          const ObTxDataSourceType &type,
          const share::ObLSID ls_id,
          const ObRegisterMdsFlag &register_flag);

  const char *get_msd_buf() { return mds_str_.ptr(); }
  int64_t get_msd_buf_len() { return mds_str_.length(); }
  const ObTxDataSourceType &get_msd_type() { return type_; }
  const share::ObLSID &get_ls_id() { return ls_id_; }
  const ObRegisterMdsFlag &get_register_flag() { return register_flag_; }

  TO_STRING_KV(K(mds_str_), "type_", to_str_mds_type(type_), K(ls_id_), K(register_flag_));

private:
  // const char *msd_buf_;
  // int64_t msd_buf_len_;
  // bool from_copy_;
  common::ObString mds_str_;
  ObTxDataSourceType type_;
  share::ObLSID ls_id_;
  ObRegisterMdsFlag register_flag_;
};

} // transaction

} // oceanbase

#endif
