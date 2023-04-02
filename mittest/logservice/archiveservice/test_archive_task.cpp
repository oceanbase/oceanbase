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

#define private public
#define protected public
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/archiveservice/ob_archive_task.h"
#include "share/scn.h"
#include "share/backup/ob_archive_piece.h"
#undef private
#undef protected
#include "share/ob_ls_id.h"
#include <cstdint>
#include <gtest/gtest.h>
namespace oceanbase
{
using namespace palf;
namespace unittest
{
using namespace oceanbase::share;
using namespace oceanbase::archive;
TEST(TestArchiveFetchTask, test_archive_fetch_task)
{
  ObArchiveLogFetchTask invalid_fetch_task;

}

TEST(TestArchiveSendTask, test_archive_send_task)
{
  const int64_t base_piece_id = 1;
  uint64_t tenant_id = 1001;
  share::SCN max_scn;
  max_scn.convert_for_logservice(10000000);
  const int64_t data_len = 1024;
  char data[data_len] = "test_data_archive";
  LSN start_offset(0);
  LSN end_offset(1024);
  ObLSID id(1001);

  share::SCN scn;
  scn.convert_for_logservice(1024000000);
  ObArchivePiece piece(scn, 10000, scn, 1);
  ObArchiveSendTask send_task;
  ArchiveWorkStation station(ArchiveKey(1, 1, 1), ObArchiveLease(1, 0, 0));
  EXPECT_EQ(OB_SUCCESS, send_task.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  start_offset = LSN(1024);
  end_offset = LSN(0);
  EXPECT_EQ(OB_INVALID_ARGUMENT, send_task.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  end_offset = LSN(1024);
  EXPECT_EQ(OB_INVALID_ARGUMENT, send_task.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  end_offset = LSN(20480);
  id = ObLSID(-1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, send_task.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  id = ObLSID(1001);
  EXPECT_EQ(OB_SUCCESS, send_task.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  ObArchiveSendTask send_task2;
  start_offset = end_offset;
  end_offset = end_offset + 1024;
  EXPECT_EQ(OB_SUCCESS, send_task2.init(tenant_id, id, station, piece, start_offset, end_offset, max_scn, data, data_len));

  EXPECT_TRUE(send_task2.is_continuous_with(send_task));

  send_task.start_offset_ = end_offset;
  EXPECT_FALSE(send_task.is_valid());
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  return 0;
}
