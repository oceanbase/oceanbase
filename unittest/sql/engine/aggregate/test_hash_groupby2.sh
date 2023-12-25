#!/bin/sh
# data format
# enum VectorFormat: uint8_t
#{
#  VEC_INVALID = 0,
#  VEC_FIXED,
#  VEC_DISCRETE,
#  VEC_CONTINUOUS,
#  VEC_UNIFORM,
#  VEC_UNIFORM_CONST,
#  VEC_MAX_FORMAT
#};
#

# batch size
batch_size_round=(1 50 150 256)
# 4 rounds test cases
round_array=(10 100 1000 10000)
# 3 data range test cases
data_range_level_array=(0 1 2)
# 3 skips_probability test cases
skips_probability_array=(0 30 80)
# 3 nulls_probability test cases
nulls_probability_array=(0 30 80)
# 6 combined data format test cases
# VEC_UNIFORM VEC_FIX
fix_data_format_array=("fix_data_format=4" "fix_data_format=1")
# VEC_UNIFORM VEC_DISCRETE VEC_CONTINUOUS
#string_data_format_array=("string_data_format=4" "string_data_format=2" "string_data_format=3")
string_data_format_array=("string_data_format=4")

test_file_prefix="./test_hash_groupby2_bg"

cfg_file="./test_hash_groupby2_bg.cfg"
origin_result_file="./origin_result_bg.data"
vec_result_file="./vec_result_bg.data"

test_case_round=1
for batch_size in ${batch_size_round[@]}
do
    for round in ${round_array[@]}
    do
        for data_range_level in ${data_range_level_array[@]}
        do
            for skips_probability in ${skips_probability_array[@]}
            do
                for nulls_probability in ${nulls_probability_array[@]}
                do
                    for fix_data_format in ${fix_data_format_array[@]}
                    do
                        for string_data_format in ${string_data_format_array[@]}
                        do
                            > ${cfg_file}

                            echo "batch_size="${batch_size} >> ${cfg_file}
                            echo "output_result_to_file=1" >> ${cfg_file}
                            echo "round="${round} >> ${cfg_file}
                            echo "data_range_level="${data_range_level} >> ${cfg_file}
                            echo "skips_probability="${skips_probability} >> ${cfg_file}
                            echo "nulls_probability="${nulls_probability} >> ${cfg_file}
                            echo ${fix_data_format} >> ${cfg_file}
                            echo ${string_data_format} >> ${cfg_file}

                            echo "###################"
                            echo "Test Case Round: "${test_case_round}
                            echo "{"
                            echo "round: "$round
                            echo "data_range_level: "${data_range_level}
                            echo "skips_probability: "${skips_probability}
                            echo "nulls_probability: "${nulls_probability}
                            echo "fix_data_format: "${fix_data_format}
                            echo "string_data_format: "${string_data_format}
                            echo "}"
                            echo "###################"

                            ./test_hash_groupby2_bg -bg

                            sort $origin_result_file -o $origin_result_file
                            sort $vec_result_file -o $vec_result_file

                            diff $origin_result_file $vec_result_file > /dev/null
                            if [ $? == 0 ]; then
                                echo "Both result file are the same!"
                            else
                                echo "Get Incorrect Result! Exit!"
                                exit
                            fi

                            test_case_round=$((test_case_round+1))
                        done
                    done
                done
            done
        done
    done
done

echo "Done"