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

#ifndef OCEANBASE_STORAGE_TX_OB_LS_TX_LOG_ADAPTER
#define OCEANBASE_STORAGE_TX_OB_LS_TX_LOG_ADAPTER

#include "share/ob_define.h"
#include "logservice/ob_log_handler.h"
#include "ob_trans_submit_log_cb.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace palf
{
class LSN;
class PalfHandle;
} // namespace palf
namespace transaction
{

class ObDupTableLSHandler;
class ObITxLogParam
{
public:
private:
  // nothing, base struct for test
};

class ObTxPalfParam : public ObITxLogParam
{
public:
  ObTxPalfParam(logservice::ObLogHandler *handler, ObDupTableLSHandler *dup_tablet_ls_handler)
      : handler_(handler), dup_tablet_ls_handler_(dup_tablet_ls_handler)
  {}
  logservice::ObLogHandler *get_log_handler() { return handler_; }
  ObDupTableLSHandler *get_dup_table_ls_handler() { return dup_tablet_ls_handler_; }

private:
  logservice::ObLogHandler *handler_;
  ObDupTableLSHandler *dup_tablet_ls_handler_;
};

class ObITxLogAdapter
{
public:
  virtual int submit_log(const char *buf,
                         const int64_t size,
                         const share::SCN &base_ts,
                         ObTxBaseLogCb *cb,
                         const bool need_nonblock) = 0;

  virtual int get_role(bool &is_leader, int64_t &epoch) = 0;
  virtual int get_max_decided_scn(share::SCN &scn) = 0;

  /**
   * Dup Table Inerface
   * */
  virtual int block_confirm_with_dup_tablet_change_snapshot(share::SCN &dup_tablet_change_snapshot);
  virtual int unblock_confirm_with_prepare_scn(const share::SCN &dup_tablet_change_snapshot,
                                               const share::SCN &prepare_scn);
  virtual int check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                                       bool &is_dup_tablet,
                                       const share::SCN &base_snapshot,
                                       const share::SCN &redo_scn);
  virtual int check_dup_tablet_readable(const ObTabletID &tablet_id,
                                        const share::SCN &read_snapshot,
                                        const bool read_from_leader,
                                        const share::SCN &max_replayed_scn,
                                        bool &readable);
  virtual int check_redo_sync_completed(const ObTransID &tx_id,
                                        const share::SCN &redo_completed_scn,
                                        bool &redo_sync_finish,
                                        share::SCN &total_max_read_version);
  virtual bool has_dup_tablet() { return false; }
  virtual int64_t get_committing_dup_trx_cnt();
};

class ObLSTxLogAdapter : public ObITxLogAdapter
{
public:
  ObLSTxLogAdapter() : log_handler_(nullptr) {}

  int init(ObITxLogParam *param);
  int submit_log(const char *buf,
                 const int64_t size,
                 const share::SCN &base_ts,
                 ObTxBaseLogCb *cb,
                 const bool need_nonblock);
  int get_role(bool &is_leader, int64_t &epoch);
  int get_max_decided_scn(share::SCN &scn);

  /**
   * Dup Table Inerface
   * */
  int block_confirm_with_dup_tablet_change_snapshot(share::SCN &dup_tablet_change_snapshot);
  int unblock_confirm_with_prepare_scn(const share::SCN &dup_tablet_change_snapshot,
                                       const share::SCN &redo_scn);
  int check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                               bool &is_dup_tablet,
                               const share::SCN &base_snapshot,
                               const share::SCN &redo_scn);
  int check_dup_tablet_readable(const ObTabletID &tablet_id,
                                const share::SCN &read_snapshot,
                                const bool read_from_leader,
                                const share::SCN &max_replayed_scn,
                                bool &readable);
  int check_redo_sync_completed(const ObTransID &tx_id,
                                const share::SCN &redo_completed_scn,
                                bool &redo_sync_finish,
                                share::SCN &total_max_read_version);
  bool has_dup_tablet();
  int64_t get_committing_dup_trx_cnt();
private:
  logservice::ObLogHandler *log_handler_;
  ObDupTableLSHandler *dup_table_ls_handler_;
};

} // namespace transaction
} // namespace oceanbase

#endif
