#include <vector>
#include <memory>
#include <limits>
#include <random>
#include <cmath>

#include "lib/codec/ob_fast_delta.h"

#include "gtest/gtest.h"

namespace oceanbase
{
namespace common
{

int do_test() {
    int N = 4096;
    uint32_t * datain = (uint32_t *)malloc(N * sizeof(uint32_t));
    uint32_t * buffer = (uint32_t *)malloc(N * sizeof(uint32_t));
    uint32_t * recovdata = (uint32_t *)malloc(N * sizeof(uint32_t));


    for (int length = 0; length <= N;) {
        printf("length = %d \n", length);
        for (uint32_t gap = 1; gap <= 387420489; gap *= 3) {
            for (int k = 0; k < length; ++k)
                datain[k] = gap * (k+1);
            compute_deltas(datain, length, buffer, 0);
            for (int k = 0; k < length; ++k) {
                if(buffer[k] != gap) {
                    printf("bug. buffer[%d]=%d expected %d \n",k,buffer[k],gap);
                    return -1;
                }
            }

            compute_prefix_sum(buffer, length, recovdata, 0);
            for(int k = 0; k < length; ++k) {
                if(datain[k] != recovdata[k]) {
                    printf("bug.\n");
                    return -1;
                }
            }
            compute_deltas_inplace(datain, length, 0);
            for (int k = 0; k < length; ++k) {
                if(datain[k] != gap) {
                    printf("bug.");
                    return -1;
                }
            }
            compute_prefix_sum_inplace(datain, length, 0);
            for(int k = 0; k < length; ++k) {
                if(datain[k] != recovdata[k]) {
                    printf("bug.\n");
                    return -1;
                }
            }
        }

        if (length < 128)
            ++length;
        else {
            length *= 2;
        }
    }
    free(datain);
    free(buffer);
    free(recovdata);
    printf("Code looks good.\n");
    return 0;
}

TEST(CompositeCodecTest, emptyArray)
{
  ASSERT_EQ(0, do_test());
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
