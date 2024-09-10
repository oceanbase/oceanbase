#ifdef OB_BUILD_CPP_ODPS
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

#include <gtest/gtest.h>
#include <fstream>
#include <stdlib.h>
#include <iostream>

#include <odps/odps_tunnel.h>

using namespace std;
using namespace apsara;
using namespace apsara::odps;
using namespace apsara::odps::sdk;

namespace test
{

class TestODPS: public ::testing::Test
{
public:
  TestODPS() {}
  ~TestODPS() {}
};

TEST_F(TestODPS, test_odps)
{
  string ep = "";
  string odpsEndpoint = "";
  string project  = "ailing_test";
  string table  = "odps_table_jim";
  uint32_t blockId = 0;

  Account account(ACCOUNT_ALIYUN, "", "");

  Configuration conf;
  conf.SetAccount(account);
  conf.SetTunnelEndpoint(ep);
  conf.SetEndpoint(odpsEndpoint);
  conf.SetUserAgent("UPLOAD_EXAMPLE");

  OdpsTunnel dt;

  try
  {
    std::vector<uint32_t> blocks;
    dt.Init(conf);
    IUploadPtr upload = dt.CreateUpload(project, table, "ds = 2, other = 'b'", "", true);
    for (blockId = 0; blockId < 2; blockId++) {
      std::cout << upload->GetStatus() << std::endl;
      IRecordWriterPtr wr = upload->OpenWriter(blockId);
      ODPSTableRecordPtr _r = upload->CreateBufferRecord();
      std::cout << _r->GetSchema()->GetColumnCount() << std::endl;
      ODPSTableRecord& r = *_r;

      for (size_t i = 0; i < 10; i++)
      {
        if (i % 10 == 0)
        {
          r.SetNullValue(0);
        }
        else
        {
          //r.SetBigIntValue(0, i);
          r.SetStringValue(0, std::string("hello") + std::to_string(i));
        }
        wr->Write(r);
      }
      wr->Close();
      std::cout << upload->GetStatus() << std::endl;
      blocks.push_back(blockId);
    }
    upload->Commit(blocks);
    std::cout << upload->GetStatus() << std::endl;
  }
  catch(OdpsException& e)
  {
    std::cerr << "OdpsTunnelException:\n" << e.what() << std::endl;
  }
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
#endif