

// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "./default_logger.h"

#include <catch2/catch_test_macros.hpp>

#include "vsag/logger.h"

TEST_CASE("test default logger", "[ut][logger]") {
    vsag::DefaultLogger logger;
    logger.SetLevel(vsag::Logger::Level::kTRACE);
    logger.Trace("this is a trace level message");
    logger.Debug("this is a debug level message");
    logger.Info("this is a info level message");
    logger.Warn("this is a warn level message");
    logger.Error("this is a error level message");
    logger.Critical("this is a critical level message");
}
