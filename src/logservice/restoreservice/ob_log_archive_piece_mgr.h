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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_ARCHIVE_PIECE_MGR_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_ARCHIVE_PIECE_MGR_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_ls_id.h"
#include "ob_log_restore_define.h"
#include <cstdint>
namespace oceanbase
{
namespace share
{
class ObArchiveLSMetaType;
}
namespace logservice
{
// Log Archive Dest is the destination for Archive and the source For Restore and Standby.
// In archive destination, logs are organized inner rounds and pieces.
//
// Round is a continuous interval of all log streams, while piece is an interval of a specified length of time
// Through parsing rounds and pieces, locate the specified log with the scn of its previous log
//
// As rounds may be continuous with previous one, forward round is necessary.

class ObLogArchivePieceContext
{
public:
  ObLogArchivePieceContext();
  ~ObLogArchivePieceContext();

public:
  // standard archive path interface
  int init(const share::ObLSID &id,
      const share::ObBackupDest &archive_dest);

  void reset();

  bool is_valid() const;

  int get_piece(const share::SCN &pre_scn,
      const palf::LSN &start_lsn,
      int64_t &dest_id,
      int64_t &round_id,
      int64_t &piece_id,
      int64_t &file_id,
      int64_t &offset,
      palf::LSN &max_lsn,
      bool &to_newest);

  // 为方便活跃文件持续消费, 将已消费记录更新到piece context
  int update_file_info(const int64_t dest_id,
      const int64_t round_id,
      const int64_t piece_id,
      const int64_t file_id,
      const int64_t file_offset,
      const palf::LSN &max_lsn);

  void get_max_file_info(int64_t &dest_id,
      int64_t &round_id,
      int64_t &piece_id,
      int64_t &max_file_id,
      int64_t &max_file_offset,
      palf::LSN &max_lsn);

  int deep_copy_to(ObLogArchivePieceContext &other);

  void reset_locate_info();

  int get_max_archive_log(palf::LSN &lsn, share::SCN &scn);

  // @brief locate the first log group entry whose log_scn equal or bigger than the input param scn
  // return the start lsn of the first log group entry
  // @param[in] scn, the pointed scn
  // @param[out] lsn, the start lsn of the returned log
  //
  // @ret_code    OB_SUCCESS    seek succeed
  //              OB_ENTRY_NOT_EXIST  log not exist
  //              other code    seek fail
  int seek(const share::SCN &scn, palf::LSN &lsn);

  // @brief get ls meta data by meta_type, including schema_meta...
  // @param[in] buf, buffer to cache read_data
  // @param[in] buf_size, the size of buffer, suggest 2MB
  // @param[in] meta_type, the type of ls meta, for example schema_meta
  // @param[in] timestamp, the upper limit meta version you want
  // @param[in] fuzzy_match, if the value if true, return the maximum version not bigger than the input timestamp;
  //            otherwise return the special version equals to the input timestamp, if the version not exists, return error_code
  // @param[out] real_size, the real_size of ls meta data
  //
  // @ret_code  OB_SUCCESS     get_data succeed
  //            OB_ENTRY_NOT_EXIST no suitable version exists
  //            other code     error unexpected
  int get_ls_meta_data(const share::ObArchiveLSMetaType &meta_type,
      const share::SCN &timestamp,
      char *buf,
      const int64_t buf_size,
      int64_t &real_size,
      const bool fuzzy_match = true);

  TO_STRING_KV(K_(is_inited), K_(locate_round), K_(id), K_(dest_id), K_(round_context),
      K_(min_round_id), K_(max_round_id), K_(archive_dest), K_(inner_piece_context));

private:
  enum class RoundOp
  {
    NONE = 0,        // no operation
    LOAD_RANGE = 1,  // get round range
    LOCATE = 2,      // locate the inital round with pre log scn
    LOAD = 3,        // get meta info of round, include piece range, scn range
    FORWARD = 4,     // switch round to the next
    BACKWARD = 5,    // switch round to the previous
  };

  enum class PieceOp {
    NONE = 0,      //  no operation
    LOAD = 1,      // refresh current piece info
    ADVANCE = 2,  // advance file range or piece status if piece is active
    FORWARD = 3,   // forward switch piece to next one(+1)
    BACKWARD = 4,  // backward switch piece to pre one(-1)
  };

  struct RoundContext
  {
    enum State : uint8_t
    {
      INVALID = 0,
      ACTIVE = 1,
      STOP = 2,
      EMPTY = 3,
    };

    State state_;
    int64_t round_id_;
    share::SCN start_scn_;              // 该轮次最小日志SCN
    share::SCN end_scn_;                // 该轮次最大日志SCN, 非STOP状态该时间为SCN_MAX
    int64_t min_piece_id_;          // 该round最小piece, 随着归档数据回收, 该值可能增大
    int64_t max_piece_id_;          // 该round最大piece, ACTIVE round该值可能增大

    int64_t base_piece_id_;
    int64_t piece_switch_interval_;//us
    share::SCN base_piece_scn_;

    RoundContext() { reset(); }
    ~RoundContext() { reset(); }

    void reset();
    bool is_valid() const;
    bool is_in_stop_state() const;
    bool is_in_empty_state() const;
    bool is_in_active_state() const;
    bool check_round_continuous_(const RoundContext &pre_round) const;

    RoundContext &operator=(const RoundContext &other);

    TO_STRING_KV(K_(state), K_(round_id), K_(start_scn), K_(end_scn), K_(min_piece_id), K_(max_piece_id),
        K_(base_piece_id), K_(piece_switch_interval), K_(base_piece_scn));
  };

  // 读到已经frozen状态的piece, 并且已经读完最后该piece最后一个文件, 则该piece文件范围信息
  // 以及最后一个文件包含的最大LSN都已确定, 如果仍然需要读, 则切piece
  struct InnerPieceContext
  {
    enum State : uint8_t
    {
      INVALID = 0,
      FROZEN = 1,       // 正常FROZEN状态的piece, 包含日志与meta文件
      EMPTY = 2,        // 当前piece是空的, 仅有meta信息没有日志, 其日志范围是确定的, 是FROZEN状态的一种特例
      LOW_BOUND = 3,    // piece存在且FROZEN, 但是日志流在该piece仍未产生, 是FROZEN状态的另一种特例
      ACTIVE = 4,       // 当前piece未冻结, 仍有日志写入
      GC = 5,           // 日志流在该piece已GC, 不需要前向再切piece, 也是FROZEN状态的一种特例
    };

    State state_;                 // 当前piece状态
    int64_t piece_id_;            // 当前所属piece id
    int64_t round_id_;            // 该piece所属轮次

    palf::LSN min_lsn_in_piece_;  // piece内最小LSN
    palf::LSN max_lsn_in_piece_;  // piece内最大LSN, 仅FROZEN状态piece该值有效
    int64_t min_file_id_;         // 当前piece最大文件id
    int64_t max_file_id_;         // 当前piece最小文件id

    int64_t file_id_;         // 当前piece已读文件最大文件id
    int64_t file_offset_;     // 当前piece已读最大文件内偏移
    palf::LSN max_lsn_;       // 当前piece已读最大文件内偏移对应日志LSN

    InnerPieceContext() { reset(); }

    void reset();
    bool is_valid() const;
    bool is_frozen_() const { return State::FROZEN == state_; }
    bool is_empty_() const { return State::EMPTY == state_; }
    bool is_low_bound_() const { return State::LOW_BOUND == state_; }
    bool is_gc_() const { return State::GC == state_; }
    bool is_active() const { return State::ACTIVE == state_; }
    int update_file(const int64_t file_id, const int64_t file_offset, const palf::LSN &lsn);
    InnerPieceContext  &operator=(const InnerPieceContext &other);

    TO_STRING_KV(K_(state), K_(piece_id), K_(round_id), K_(min_lsn_in_piece), K_(max_lsn_in_piece),
        K_(min_file_id), K_(max_file_id), K_(file_id), K_(file_offset), K_(max_lsn));
  };

private:
  int get_piece_(const share::SCN &scn,
      const palf::LSN &lsn,
      const int64_t file_id,
      int64_t &dest_id,
      int64_t &round_id,
      int64_t &piece_id,
      int64_t &offset,
      palf::LSN &max_lsn,
      bool &to_newest);

  // 获取归档源基本元信息
  virtual int load_archive_meta_();

  // 获取round范围
  virtual int get_round_range_();

  // 根据时间戳定位round
  virtual int get_round_(const share::SCN &scn);

  // 如果round不满足数据需求, 支持切round
  int switch_round_if_need_(const share::SCN &scn, const palf::LSN &lsn);

  void check_if_switch_round_(const share::SCN &scn, const palf::LSN &lsn, RoundOp &op);
  bool is_max_round_done_(const palf::LSN &lsn) const;
  bool need_backward_round_(const palf::LSN &lsn) const;
  bool need_forward_round_(const palf::LSN &lsn) const;
  bool need_load_round_info_(const share::SCN &scn, const palf::LSN &lsn) const;

  // 获取指定round元信息
  virtual int load_round_(const int64_t round_id, RoundContext &round_context, bool &exist);

  // round可能不连续, 检查round是否存在
  virtual int check_round_exist_(const int64_t round_id, bool &exist);

  int load_round_info_();
  virtual int get_round_piece_range_(const int64_t round_id, int64_t &min_piece_id, int64_t &max_piece_id);

  // 前后向切round
  int forward_round_(const RoundContext &pre_round);
  int backward_round_();

  // 当piece不匹配, 需要切piece
  int switch_piece_if_need_(const int64_t file_id, const share::SCN &scn, const palf::LSN &lsn);
  void check_if_switch_piece_(const int64_t file_id, const palf::LSN &lsn, PieceOp &op);

  // load当前piece信息, 包括piece内文件范围, LSN范围
  int get_cur_piece_info_(const share::SCN &scn);
  int advance_piece_();
  virtual int get_piece_meta_info_(const int64_t piece_id);
  int get_ls_inner_piece_info_(const share::ObLSID &id, const int64_t dest_id, const int64_t round_id,
      const int64_t piece_id, palf::LSN &min_lsn, palf::LSN &max_lsn, bool &exist, bool &gc);
  virtual int get_piece_file_range_();

  int forward_piece_();
  int backward_piece_();
  int cal_load_piece_id_(const share::SCN &scn, int64_t &piece_id);

  int64_t cal_piece_id_(const share::SCN &scn) const;
  virtual int get_min_lsn_in_piece_();
  int64_t cal_archive_file_id_(const palf::LSN &lsn) const;

  int get_(const palf::LSN &lsn,
      const int64_t file_id,
      int64_t &dest_id,
      int64_t &round_id,
      int64_t &piece_id,
      int64_t &offset,
      palf::LSN &max_lsn,
      bool &done,
      bool &to_newest);

  int get_max_archive_log_(const ObLogArchivePieceContext &origin, palf::LSN &lsn, share::SCN &scn);

  int get_max_log_in_round_(const ObLogArchivePieceContext &origin,
      const int64_t round_id,
      palf::LSN &lsn,
      share::SCN &scn,
      bool &exist);

  int get_max_log_in_piece_(const ObLogArchivePieceContext &origin,
      const int64_t round_id,
      const int64_t piece_id,
      palf::LSN &lsn,
      share::SCN &scn,
      bool &exist);

  int get_max_log_in_file_(const ObLogArchivePieceContext &origin,
      const int64_t round_id,
      const int64_t piece_id,
      const int64_t file_id,
      palf::LSN &lsn,
      share::SCN &scn,
      bool &exist);

  int seek_(const share::SCN &scn, palf::LSN &lsn);
  int seek_in_piece_(const share::SCN &scn, palf::LSN &lsn);
  int seek_in_file_(const int64_t file_id, const share::SCN &scn, palf::LSN &lsn);

  int read_part_file_(const int64_t round_id,
      const int64_t piece_id,
      const int64_t file_id,
      const int64_t file_offset,
      char *buf,
      const int64_t buf_size,
      int64_t &read_size);

  int extract_file_base_lsn_(const char *buf,
      const int64_t buf_size,
      palf::LSN &base_lsn);
  int get_ls_meta_data_(const share::ObArchiveLSMetaType &meta_type,
      const share::SCN &timestamp,
      const bool fuzzy_match,
      char *buf,
      const int64_t buf_size,
      int64_t &real_size);
  int get_ls_meta_in_piece_(const share::ObArchiveLSMetaType &meta_type,
      const share::SCN &timestamp,
      const bool fuzzy_match,
      const int64_t base_piece_id,
      char *buf,
      const int64_t buf_size,
      int64_t &real_size);
  int get_ls_meta_file_in_array_(const share::SCN &timestamp,
      const bool fuzzy_match,
      int64_t &file_id,
      common::ObIArray<int64_t> &array);

private:
  bool is_inited_;
  bool locate_round_;
  share::ObLSID id_;
  int64_t dest_id_;
  int64_t min_round_id_;
  int64_t max_round_id_;
  RoundContext round_context_;
  InnerPieceContext inner_piece_context_;

  share::ObBackupDest archive_dest_;
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_ARCHIVE_PIECE_MGR_H_ */
