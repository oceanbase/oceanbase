// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <iostream>
#include <cstdlib>
#include <random>
#include <cmath>
#include <boost/program_options.hpp>

#include "utils.h"

namespace po = boost::program_options;

int block_write_float(std::ofstream &writer, size_t ndims, size_t npts, float norm)
{
    auto vec = new float[ndims];

    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution<> normal_rand{0, 1};

    for (size_t i = 0; i < npts; i++)
    {
        float sum = 0;
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = (float)normal_rand(gen);
        for (size_t d = 0; d < ndims; ++d)
            sum += vec[d] * vec[d];
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = vec[d] * norm / std::sqrt(sum);

        writer.write((char *)vec, ndims * sizeof(float));
    }

    delete[] vec;
    return 0;
}

int block_write_int8(std::ofstream &writer, size_t ndims, size_t npts, float norm)
{
    auto vec = new float[ndims];
    auto vec_T = new int8_t[ndims];

    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution<> normal_rand{0, 1};

    for (size_t i = 0; i < npts; i++)
    {
        float sum = 0;
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = (float)normal_rand(gen);
        for (size_t d = 0; d < ndims; ++d)
            sum += vec[d] * vec[d];
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = vec[d] * norm / std::sqrt(sum);

        for (size_t d = 0; d < ndims; ++d)
        {
            vec_T[d] = (int8_t)std::round(vec[d]);
        }

        writer.write((char *)vec_T, ndims * sizeof(int8_t));
    }

    delete[] vec;
    delete[] vec_T;
    return 0;
}

int block_write_uint8(std::ofstream &writer, size_t ndims, size_t npts, float norm)
{
    auto vec = new float[ndims];
    auto vec_T = new int8_t[ndims];

    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution<> normal_rand{0, 1};

    for (size_t i = 0; i < npts; i++)
    {
        float sum = 0;
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = (float)normal_rand(gen);
        for (size_t d = 0; d < ndims; ++d)
            sum += vec[d] * vec[d];
        for (size_t d = 0; d < ndims; ++d)
            vec[d] = vec[d] * norm / std::sqrt(sum);

        for (size_t d = 0; d < ndims; ++d)
        {
            vec_T[d] = 128 + (int8_t)std::round(vec[d]);
        }

        writer.write((char *)vec_T, ndims * sizeof(uint8_t));
    }

    delete[] vec;
    delete[] vec_T;
    return 0;
}

int main(int argc, char **argv)
{
    std::string data_type, output_file;
    size_t ndims, npts;
    float norm;

    try
    {
        po::options_description desc{"Arguments"};

        desc.add_options()("help,h", "Print information on arguments");

        desc.add_options()("data_type", po::value<std::string>(&data_type)->required(), "data type <int8/uint8/float>");
        desc.add_options()("output_file", po::value<std::string>(&output_file)->required(),
                           "File name for saving the random vectors");
        desc.add_options()("ndims,D", po::value<uint64_t>(&ndims)->required(), "Dimensoinality of the vector");
        desc.add_options()("npts,N", po::value<uint64_t>(&npts)->required(), "Number of vectors");
        desc.add_options()("norm", po::value<float>(&norm)->required(), "Norm of the vectors");
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        if (vm.count("help"))
        {
            std::cout << desc;
            return 0;
        }
        po::notify(vm);
    }
    catch (const std::exception &ex)
    {
        std::cerr << ex.what() << '\n';
        return -1;
    }

    if (data_type != std::string("float") && data_type != std::string("int8") && data_type != std::string("uint8"))
    {
        std::cout << "Unsupported type. float, int8 and uint8 types are supported." << std::endl;
        return -1;
    }

    if (norm <= 0.0)
    {
        std::cerr << "Error: Norm must be a positive number" << std::endl;
        return -1;
    }

    if (data_type == std::string("int8") || data_type == std::string("uint8"))
    {
        if (norm > 127)
        {
            std::cerr << "Error: for int8/uint8 datatypes, L2 norm can not be "
                         "greater "
                         "than 127"
                      << std::endl;
            return -1;
        }
    }

    try
    {
        std::ofstream writer;
        writer.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        writer.open(output_file, std::ios::binary);
        auto npts_u32 = (uint32_t)npts;
        auto ndims_u32 = (uint32_t)ndims;
        writer.write((char *)&npts_u32, sizeof(uint32_t));
        writer.write((char *)&ndims_u32, sizeof(uint32_t));

        size_t blk_size = 131072;
        size_t nblks = ROUND_UP(npts, blk_size) / blk_size;
        std::cout << "# blks: " << nblks << std::endl;

        int ret = 0;
        for (size_t i = 0; i < nblks; i++)
        {
            size_t cblk_size = std::min(npts - i * blk_size, blk_size);
            if (data_type == std::string("float"))
            {
                ret = block_write_float(writer, ndims, cblk_size, norm);
            }
            else if (data_type == std::string("int8"))
            {
                ret = block_write_int8(writer, ndims, cblk_size, norm);
            }
            else if (data_type == std::string("uint8"))
            {
                ret = block_write_uint8(writer, ndims, cblk_size, norm);
            }
            if (ret == 0)
                std::cout << "Block #" << i << " written" << std::endl;
            else
            {
                writer.close();
                std::cout << "failed to write" << std::endl;
                return -1;
            }
        }
        writer.close();
    }
    catch (const std::exception &e)
    {
        std::cout << std::string(e.what()) << std::endl;
        diskann::cerr << "Index build failed." << std::endl;
        return -1;
    }

    return 0;
}
