/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_AI_GATEWAY_ENDPOINT_STAT_H_
#define OB_ALL_VIRTUAL_AI_GATEWAY_ENDPOINT_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{
namespace share
{
class ObAiEndpointCircuitState;
}
namespace observer
{

class ObAllVirtualAiGatewayEndpointStat : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualAiGatewayEndpointStat();
  virtual ~ObAllVirtualAiGatewayEndpointStat();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  int init(const common::ObAddr &addr);

private:
  enum COLUMN_ID
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    GATEWAY_NAME,
    ENDPOINT_NAME,
    ROUTING_STATUS,
    FAILURE_RATE,
    TOTAL_REQUESTS,
    BLOCKED_UNTIL_TS,
    LAST_FAILED_TS
  };

  struct EndpointStatInfo
  {
    uint64_t tenant_id_;
    char gateway_name_[share::OB_MAX_AI_GATEWAY_NAME_LENGTH + 1];
    char endpoint_name_[share::OB_MAX_AI_GATEWAY_ENDPOINT_NAME_LENGTH + 1];
    const char *routing_status_;
    int64_t failure_rate_;
    int64_t total_requests_;
    int64_t block_until_us_;       // 0 means NULL
    int64_t last_failure_time_us_; // 0 means NULL

    EndpointStatInfo()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        routing_status_("SERVING"),
        failure_rate_(0),
        total_requests_(0),
        block_until_us_(0),
        last_failure_time_us_(0)
    {
      gateway_name_[0] = '\0';
      endpoint_name_[0] = '\0';
    }

    TO_STRING_KV(K_(tenant_id), K_(gateway_name), K_(endpoint_name), K_(routing_status),
                 K_(failure_rate), K_(total_requests), K_(block_until_us), K_(last_failure_time_us));
  };

  bool is_inited_;
  common::ObAddr addr_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];

  common::ObArray<EndpointStatInfo> stat_infos_;
  int64_t stat_pos_;
  bool data_collected_;

  int collect_data_();
  static void build_endpoint_stat_info(uint64_t tenant_id,
                                         const common::ObString &gateway_name,
                                         const share::ObAiGatewayEndpoint &ep,
                                         share::ObAiEndpointCircuitState *ep_state,
                                         const share::ObAiCircuitBreakerParams &cb_params,
                                         EndpointStatInfo &info);

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualAiGatewayEndpointStat);
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_AI_GATEWAY_ENDPOINT_STAT_H_ */
