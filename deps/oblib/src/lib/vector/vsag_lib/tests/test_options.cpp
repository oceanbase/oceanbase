
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

#include <catch2/catch_test_macros.hpp>
#include <iostream>

#include "vsag/options.h"

void
info(const std::string& msg) {
    vsag::Options::Instance().logger()->Info(msg);
}

TEST_CASE("set external logger", "[ft][options]") {
    class MyLogger : public vsag::Logger {
    public:
        inline void
        SetLevel(Level log_level) override {
            level_ = log_level - vsag::Logger::Level::kTRACE;
        }

        inline void
        Trace(const std::string& msg) override {
            if (level_ <= 0) {
                std::cout << "[trace]" << msg << std::endl;
            }
        }

        inline void
        Debug(const std::string& msg) override {
            if (level_ <= 1) {
                std::cout << "[debug]" << msg << std::endl;
            }
        }

        inline void
        Info(const std::string& msg) override {
            if (level_ <= 2) {
                std::cout << "[info]" << msg << std::endl;
            }
        }

        inline void
        Warn(const std::string& msg) override {
            if (level_ <= 3) {
                std::cout << "[warn]" << msg << std::endl;
            }
        }

        inline void
        Error(const std::string& msg) override {
            if (level_ <= 4) {
                std::cout << "[error]" << msg << std::endl;
            }
        }

        void
        Critical(const std::string& msg) override {
            if (level_ <= 5) {
                std::cout << "[critical]" << msg << std::endl;
            }
        }

        int64_t level_ = 0;
    };

    MyLogger logger;
    info("test test, by default logger");
    vsag::Options::Instance().set_logger(&logger);
    info("test test, by my logger");
    vsag::Options::Instance().set_logger(nullptr);
}
