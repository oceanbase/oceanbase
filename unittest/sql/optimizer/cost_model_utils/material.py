from mylog.mylog import MyLogger
from op_generator import op_generator
from cost_test_conf import Config
import subprocess as sp
import os
from lmfit import Model
import numpy as np

# step 1 gen op and conf
material_cls = op_generator.gen_operator("material")
conf = Config()
conf.u_to_test_op_c = 'material'
conf.is_not_running_as_unittest_c = True
conf.schema_file_c = 'c10k1.schema'
conf.row_count_c = 1000
conf.input_projector_count_c = 1

material_op = material_cls(conf)
result_file_name = 'material_result'
if os.path.exists(result_file_name):
    os.remove(result_file_name)

# step 2 do_bench and gen data
row_count_max = 1001
row_count_step = 100

column_counts = [3, 5, 8]

case_run_time = 7

total_case_count = (row_count_max / row_count_step + 1) * len(column_counts) * case_run_time
case_count = 0

print "Total case count %s ..." % (total_case_count)
for row_count in xrange(1, row_count_max + 1, row_count_step):
    for column_count in column_counts:
        for time in xrange(case_run_time):
            case_count += 1
            material_op.conf.row_count_c = row_count
            material_op.conf.input_projector_count_c = column_count
            sp.check_call("echo -n '%s,' >> %s" % (row_count, result_file_name), shell=True)
            sp.check_call("echo -n '%s,' >> %s" % (column_count, result_file_name), shell=True)
            print "Running case %s / %s ... : %s " % (case_count, total_case_count, material_op.get_bench_cmd())
            print "%s >> %s" % (material_op.get_bench_cmd(), result_file_name)
            sp.check_call("%s >> %s" % (material_op.get_bench_cmd(), result_file_name), shell=True)

# step 3 preprocess data
final_file_name = "material_result_final"
if os.path.exists("material_final_result"):
    os.remove("material_final_result")
data_cmd = material_op.get_data_preprocess_cmd()
sp.check_call(data_cmd, shell=True)

# step 4 fit and output
# given model form, do fit using previous result data
# case param should be considered with cost_model_util.cpp output format
# eg: material_test() in cost_model_util.cpp
#     output row_count, cost_time
out_model_file_name = "material_model"
if os.path.exists(out_model_file_name):
    os.remove(out_model_file_name)


def material_model_form(args,
                        # Tstartup,
                        Trow_once,
                        Trow_col):
    (
        Nrow,
        Ncol,
    ) = args

    total_cost = 0  # Tstartup
    total_cost += Nrow * (Trow_once + Ncol * Trow_col)
    return total_cost


def material_model_arr(arg_sets,
                       # Tstartup,
                       Trow_once,
                       Trow_col):
    res = []
    for single_arg_set in arg_sets:
        res.append(material_model_form(single_arg_set,
                                       # Tstartup,
                                       Trow_once,
                                       Trow_col))
    return np.array(res)


def extract_info_from_line(line):
    splited = line.split(",")
    line_info = []
    for item in splited:
        line_info.append(float(item))
    return line_info


material_model = Model(material_model_arr)
material_model.set_param_hint("Trow_once", min=0.0)
material_model.set_param_hint("Trow_col", min=0.0)
file = open(final_file_name, "r")
arg_sets = []
times = []
case_params = []
for line in file:
    if line.startswith('#'):
        continue
    case_param = extract_info_from_line(line)
    case_params.append(case_param)
    arg_sets.append((case_param[0], case_param[1]))
    times.append(case_param[3])
file.close()
arg_sets_np = np.array(arg_sets)
times_np = np.array(times)
# result is the fitting result model
result = material_model.fit(times_np, arg_sets=arg_sets_np,
                            # Tstartup=10.0,
                            Trow_once=10.0,
                            Trow_col=1.0
                            )

# res_line = str(result.best_values["Tstartup"]) + ","
res_line = str(result.best_values["Trow_once"]) + ","
res_line += str(result.best_values["Trow_col"])

print result.fit_report()

if out_model_file_name:
    out_file = open(out_model_file_name, "w")
    out_file.write(res_line)
    out_file.close()
