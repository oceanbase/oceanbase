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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{

struct ObDiskStat final
{
  int64_t disk_idx_;
  int64_t install_seq_;
  int64_t create_ts_;
  int64_t finish_ts_;
  int64_t percent_;
  const char *status_;
  char alias_name_[common::MAX_PATH_SIZE];

  ObDiskStat();
  TO_STRING_KV(K_(disk_idx), K_(install_seq), K_(create_ts), K_(finish_ts), K_(percent),
      K_(status), K_(alias_name));
};

struct ObDiskStats final
{
  ObArray<ObDiskStat> disk_stats_;
  int64_t data_num_;
  int64_t parity_num_;

  ObDiskStats();
  void reset();
  TO_STRING_KV(K_(data_num), K_(parity_num), K_(disk_stats));
};

class ObAllVirtualRaidStat: public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DISK_INDEX,
    INSTALL_SEQ,
    DATA_NUM,
    PARITY_NUM,
    CREATE_TS,
    FINISH_TS,
    ALIAS_NAME,
    STATUS,
    PERCENT,
  };
public:
  ObAllVirtualRaidStat();
  virtual ~ObAllVirtualRaidStat();
  int init(const common::ObAddr &addr);
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  ObDiskStats disk_stats_;
  int64_t cur_idx_;
  common::ObAddr addr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualRaidStat);
};

}
}



#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_STAT_H_ */
