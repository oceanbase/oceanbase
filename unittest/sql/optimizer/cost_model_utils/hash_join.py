from mylog.mylog import MyLogger
from op_generator import op_generator
from cost_test_conf import Config
import subprocess as sp
import os
from lmfit import Model
import numpy as np

hash_cls = op_generator.gen_operator("hash_join")
conf = Config()
conf.u_to_test_op_c = 'hash'
conf.is_not_running_as_unittest_c = True
conf.schema_file_c = 'c10k1x2.schema'
conf.left_row_count_c = 1000
conf.right_row_count_c = 1000
conf.left_min_c = 1
conf.right_min_c = 1
conf.is_random_c = True
conf.left_pj_c = 10
conf.right_pj_c = 10
hash_op = hash_cls(conf)
result_file_name = "hash_join_result"
if os.path.exists(result_file_name):
    os.remove(result_file_name)

# step 2 do bench and gen data

case_run_time = 7
case_count = 0
row_count_max = 100000;
row_count_step = 2000;
total_case_count = row_count_max/row_count_step
total_case_count *= total_case_count

print "Total case count %s ..." % (total_case_count)
for left_row_count in xrange(1000, row_count_max + 1, row_count_step):
     for right_row_count in xrange(1000, row_count_max + 1, row_count_step):
         case_count+=1
         hash_op.conf.left_row_count_c = left_row_count
         hash_op.conf.right_row_count_c = right_row_count
         hash_op.conf.left_max_c = max(left_row_count, right_row_count) * 3
         hash_op.conf.right_max_c = hash_op.conf.left_max_c
         sp.check_call("echo -n '%s,%s,' >> %s" % (left_row_count, right_row_count, result_file_name), shell=True)
         print "Running case %s / %s ... : %s " % (case_count, total_case_count, hash_op.get_bench_cmd())
         print "%s >> %s" % (hash_op.get_bench_cmd(), result_file_name)
         sp.check_call("%s >> %s" % (hash_op.get_bench_cmd(), result_file_name), shell=True)

# step 3 process data
final_file_name = "hash_join_result_final"
if os.path.exists(final_file_name):
    os.remove(final_file_name)

data_cmd = hash_op.get_data_preprocess_cmd()
sp.check_call(data_cmd, shell=True)

# step 4 fit and output

out_model_file_name = "hash_model"
if os.path.exists(out_model_file_name):
    os.remove(out_model_file_name)


def hash_model_form(args,
                    Tstart_up,
                    Tbuild_htable,
                    Tright_row_once,
                    Tconvert_tuple,
                    #Tequal_cond,
                    #Tfilter_cond,
                    Tjoin_row
                    ):
    (
        Nres_row,
        Nleft_row,
        Nright_row,
        Nequal_cond,
    ) = args
    total_cost = Tstart_up  # Tstartup
    total_cost += Nleft_row * Tbuild_htable
    total_cost += Nright_row * Tright_row_once
    total_cost += Nequal_cond * Tconvert_tuple
    total_cost += Nres_row * Tjoin_row
    return total_cost


def hash_model_arr(arg_sets,
                   Tstart_up,
                   Tbuild_htable,
                   Tright_row_once,
                   Tconvert_tuple,
                   #Tequal_cond,
                   #Tfilter_cond,
                   Tjoin_row):
    res = []
    for single_arg_set in arg_sets:
        res.append(hash_model_form(single_arg_set,
                                   Tstart_up,
                                   Tbuild_htable,
                                   Tright_row_once,
                                   Tconvert_tuple,
                                   #Tequal_cond,
                                   #Tfilter_cond,
                                   Tjoin_row))
    return np.array(res)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info

hash_model = Model(hash_model_arr)
hash_model.set_param_hint("Tstart_up", min=0.0)
hash_model.set_param_hint("Tbuild_htable", min=0.0)
hash_model.set_param_hint("Tright_row_once", min=0.0)
hash_model.set_param_hint("Tconvert_tuple", min=0.0)
hash_model.set_param_hint("Tjoin_row", min=0.0)
file = open(final_file_name, "r")
arg_sets = []
times = []
case_params = []
for line in file:
    if line.startswith('#'):
        continue
    case_param = extract_info_from_line(line)
    case_params.append(case_param)
    arg_sets.append((case_param[2], case_param[0], case_param[1], case_param[3]))
    times.append(case_param[4])
file.close()
arg_sets_np = np.array(arg_sets)
times_np = np.array(times)

result = hash_model.fit(times_np, arg_sets=arg_sets_np,
                        Tstartup=0.0,
                        Tbuild_htable=0.0,
                        Tright_row_once=0.0,
                        Tconvert_tuple=0.0,
                        #Tequal_cond=0.0,
                        #Tfilter_cond=0.0,
                        Tjoin_row=0.0)
res_line = str(result.best_values["Tstart_up"]) + ","
res_line += str(result.best_values["Tbuild_htable"]) + ","
res_line += str(result.best_values["Tright_row_once"]) + ","
res_line += str(result.best_values["Tconvert_tuple"]) + ","
#res_line += str(result.best_values["Tequal_cond"]) + ","
#res_line += str(result.best_values["Tfilter_cond"]) + ","
res_line += str(result.best_values["Tjoin_row"])
print result.fit_report()

if out_model_file_name:
    out_file = open(out_model_file_name, "w")
    out_file.write(res_line)
    out_file.close()
