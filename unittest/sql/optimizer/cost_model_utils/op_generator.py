from cost_test_conf import Config
from mylog.mylog import MyLogger
import subprocess as sp


def init_func(self, conf):
    self.conf = conf


def get_bench_cmd(self):
    cmd = './cost_model_util ' + self.conf.gen_params()
    return cmd

def get_data_preprocess_cmd(self):
    cmd = 'python preprocess.py -i {0} -o {1} -d'.format(
        self.__class__.__name__ + '_result',
        self.__class__.__name__ + '_result_final'
    )
    return cmd

def do_bench(self):
    MyLogger.info(self.conf)
    cmd = self.get_bench_cmd()
    MyLogger.info(cmd)
    sp.check_call(cmd, shell=True)
    data_cmd = self.get_data_preprocess_cmd()
    sp.check_call(data_cmd, shell=True)


class op_generator(object):
    op_dict = {}
    '''
    name if type is not None name = operatorname + test_type_name
    '''

    @staticmethod
    def gen_operator(name):
        if op_generator.op_dict.has_key(name):
            return op_generator.op_dict[name]
        else:
            cls = type(name, (object,), {'__init__': init_func, 'do_bench': do_bench,
                                         'get_bench_cmd': get_bench_cmd,
                                         'get_data_preprocess_cmd': get_data_preprocess_cmd})
            op_generator.op_dict[name] = cls
            return cls


if __name__ == '__main__':
    ##mat related conf
    material_cls = op_generator.gen_operator('material')
    conf = Config()
    conf.u_to_test_op_c = 'material'
    conf.is_not_running_as_unittest_c = True
    conf.schema_file_c = 'c10k1.schema'
    conf.row_count_c = 1000
    conf.input_projector_count_c = 1

    material_op = material_cls(conf)
    material_op.do_bench()
