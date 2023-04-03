from mylog.mylog import MyLogger
import subprocess as sp
'''
class Tester(object):
    bench_script = "python benchmaster_{0}.py"
    data_process_script = 'python preprocess.py -i {0} -o {1} -d'
    fit_script = 'python fit_{0}.py'

    def __init__(self, conf):
        self.conf = conf

    def do_all(self):
        # MyLogger.log('try to do all test fit plot')
        pass

    def do_bench(self):
        # MyLogger.log('try to do bench')
        sp.check_call(Tester.bench_script.format(self.conf.u_to_test_op_c), shell=True)

    def do_fit(self):
        # MyLogger.log('try to do fit')
        sp.check_call(Tester.fit_script.format(self.conf.u_to_test_op_c), shell=True)

    def do_plot(self):
        # MyLogger.log('try to do plot')
        pass

    def do_data_process(self):
        if self.conf.u_to_test_type_c is None:
            sp.check_call(Tester.data_process_script.format(self.conf.u_to_test_op_c + '_result',
                                                            self.conf.u_to_test_op_c + '_result_final'), shell=True)
        else:
            sp.check_call(
                Tester.data_process_script.format(self.conf.u_to_test_op + '_' + self.conf.u_to_test_type + '_result',
                                                  self.conf.u_to_test_op + '_' + self.conf.u_to_test_type + '_result_final'
                                                  ), shell=True)
'''


if __name__ == '__main__':
    MyLogger.info("start to do cost model unittest")
    sp.check_call('python %s' % ('material.py'), shell=True)
