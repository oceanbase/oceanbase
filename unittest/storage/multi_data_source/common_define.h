#ifndef UNITTEST_STORAGE_MULTI_DATA_SOURCE_COMMON_DEFINE_H
#define UNITTEST_STORAGE_MULTI_DATA_SOURCE_COMMON_DEFINE_H
#include "src/share/scn.h"
#include "example_user_data_define.h"

namespace oceanbase {
namespace unittest {

inline share::SCN mock_scn(int64_t val) { share::SCN scn; scn.convert_for_gts(val); return scn; }

}
}
#endif