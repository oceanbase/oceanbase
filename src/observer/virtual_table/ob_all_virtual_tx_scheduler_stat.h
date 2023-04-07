#ifndef OB_ALL_VIRTUAL_TX_SCHEDULER_STAT_H_
#define OB_ALL_VIRTUAL_TX_SCHEDULER_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/tx/ob_tx_stat.h"
#include "common/ob_clock_generator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace transaction
{
  class ObTxSchedulerStat;
}
namespace observer
{

class ObGVTxSchedulerStat: public common::ObVirtualTableScannerIterator,
                           public omt::ObMultiTenantOperator
{
public:
  ObGVTxSchedulerStat();
  ~ObGVTxSchedulerStat();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row) { return execute(row);}
  virtual void reset();

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_tx_info_(transaction::ObTxSchedulerStat &tx_scheduler_stat);
  bool is_valid_timestamp_(const int64_t timestamp) const;

private:
  enum
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    SESSION_ID,
    TX_ID,
    STATE,
    CLUSTER_ID,
    COORDINATOR,
    PARTICIPANTS,
    ISOLATION_LEVEL,
    SNAPSHOT_VERSION,
    ACCESS_MODE,
    TX_OP_SN,
    FLAG,
    ACTIVE_TS,
    EXPIRE_TS,
    TIMEOUT_US,
    REF_CNT,
    TX_DESC_ADDR,
    SAVEPOINTS,
    SAVEPOINTS_TOTAL_CNT,
    INTERNAL_ABORT_CAUSE,
    CAN_EARLY_LOCK_RELEASE,
    GTRID,
    BQUAL,
    FORMAT_ID
  };
  static const int64_t OB_MAX_BUFFER_SIZE = 1024;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char parts_buffer_[OB_MAX_BUFFER_SIZE];
  char tx_desc_addr_buffer_[20];
  char savepoints_buffer_[OB_MAX_BUFFER_SIZE];
  transaction::ObXATransID xid_;
  transaction::ObTxSchedulerStatIterator tx_scheduler_stat_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObGVTxSchedulerStat);
};

}
}
#endif /* OB_ALL_VIRTUAL_TX_SCHEDULER_STAT_H_ */
