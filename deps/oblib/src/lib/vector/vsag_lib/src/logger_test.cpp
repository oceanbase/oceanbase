

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

#include "./logger.h"

#include <catch2/catch_test_macros.hpp>

#include "spdlog/spdlog.h"

TEST_CASE("test logger", "[ut][logger]") {
    vsag::logger::set_level(vsag::logger::level::trace);
    vsag::logger::trace("this is a trace level message");
    vsag::logger::debug("this is a debug level message");
    vsag::logger::info("this is a info level message");
    vsag::logger::warn("this is a warn level message");
    vsag::logger::error("this is a error level message");
    vsag::logger::critical("this is a critical level message");
}

TEST_CASE("spdlog usage", "[ut][log]") {
    spdlog::info("Welcome to spdlog!");
    spdlog::error("SomeError message with arg: {}", 1);

    spdlog::warn("Easy padding in numbers like {:08d}", 12);
    spdlog::critical("Support for int: {0:d};  hex: {0:x};  oct: {0:o}; bin: {0:b}", 42);
    spdlog::info("Support for floats {:03.2f}", 1.23456);
    spdlog::info("Positional args are {1} {0}..", "too", "supported");
    spdlog::info("{:<30}", "left aligned");

    spdlog::set_level(spdlog::level::debug);  // Set global log level to debug
    spdlog::debug("This message should be displayed..");

    // change log pattern
    spdlog::set_pattern("[%H:%M:%S %z] [%n] [%^---%L---%$] [thread %t] %v");

    // Compile time log levels
    // define SPDLOG_ACTIVE_LEVEL to desired level
    SPDLOG_TRACE("Some trace message with param {}", 42);
    SPDLOG_DEBUG("Some debug message");
}
