/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_TRANS_CTRL_H
#define _OB_TABLE_TRANS_CTRL_H

#include "observer/table/utils/ob_table_trans_utils.h"


namespace oceanbase
{
namespace table
{
class ObTableCreateCbFunctor;

class ObTableTransCtrl final
{
public:
  static const uint32_t OB_KV_DEFAULT_SESSION_ID = 0;
public:
  static int start_trans(ObTableTransParam &trans_param);
  static int end_trans(ObTableTransParam &trans_param);
private:
  static int setup_tx_snapshot(ObTableTransParam &trans_param);
  static int async_commit_trans(ObTableTransParam &trans_param);
  static int init_read_trans(ObTableTransParam &trans_param);
  static void release_read_trans(transaction::ObTxDesc *&trans_desc);
  static int sync_end_trans(ObTableTransParam &trans_param);
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_TRANS_CTRL_H */
