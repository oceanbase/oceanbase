class Config(object):
    '''
    user input info
    '''
    ################
    operators = {
        'array': 'array',
        'material': 'material',
        'mergegroupby': 'mergegroupby',
        'merge': 'merge',
        'hash': 'hash',
        'miss': 'miss',
        'nl': 'nl',
        'rowstore': 'rowstore',
        'sort_add': 'sort_add',
        'sort': 'sort',
        'sort_with_type': 'sort_with_type'
    }
    types_to_test = {'bigint': 'bigint', 'double': 'double', 'float': 'float', 'timestamp': 'timestamp',
                     'number': 'number(20,3)', 'v32': 'varchar(32)', 'v64': 'varchar(64)', 'v128': 'varchar(128)'}
    config_map_dict = {
        'is_printing_help_c': ' -h ',
        'schema_file_c': ' -s ',
        'row_count_c': ' -r ',
        'left_row_count_c': ' -r ',
        'right_row_count_c': ' -r ',
        'sort_col_count_c': ' -c ',
        'input_projector_count_c': ' -p ',
        'left_pj_c': ' -p ',
        'right_pj_c': ' -p ',
        'is_printing_output_c': ' -O ',
        'equal_cond_count_c': ' -e ',
        'other_cond_count_c': ' -o ',
        'u_to_test_op_c': ' -t ',
        'u_to_test_type_c': '',
        'is_binding_cpu_c': ' -B ',
        'seed_min_c': ' -Z ',
        'left_min_c': ' -Z ',
        'right_min_c': ' -Z ',
        'seed_max_c': ' -X ',
        'left_max_c': '-X',
        'right_max_c': '-X',
        'seed_step_c': ' -C ',
        'left_seed_step_c': ' -C ',
        'right_seed_step_c': ' -C ',
        'seed_step_len_c': ' -V ',
        'left_seed_step_len_c': ' -V ',
        'right_seed_step_len_c': ' -V ',
        'limit_c': ' -L ',
        'is_random_c': ' -R ',
        'is_experimental_c': ' -K ',
        'sleep_before_test_c': ' -S ',
        'add_sort_column_c': ' -T ',
        'info_type_c': ' -i ',
        'common_prefix_len_c': ' -l ',
        'is_not_running_as_unittest_c': ' -G '
    }

    def __init__(self):
        # config info based on cost_model_util.cpp
        self.is_printing_help_c = False
        self.schema_file_c = None
        self.row_count_c = None
        self.left_row_count_c = None
        self.right_row_count_c = None
        self.sort_col_count_c = None
        self.input_projector_count_c = None
        self.left_pj = None
        self.right_pj = None
        self.is_printing_output_c = False
        self.equal_cond_count_c = None
        self.other_cond_count_c = None
        self.u_to_test_op_c = None
        self.u_to_test_type_c = None  # special
        self.is_binding_cpu_c = False
        self.seed_min_c = None
        self.left_min_c = None
        self.rigt_min_c = None
        self.seed_max_c = None
        self.left_max_c = None
        self.right_max_c = None
        self.seed_step_c = None
        self.left_seed_step_c = None
        self.right_seed_step_c = None
        self.seed_step_len_c = None
        self.left_seed_step_len_c = None
        self.right_seed_step_len_c = None
        self.limit_c = None
        self.is_random_c = False
        self.is_experimental_c = False
        self.sleep_before_test_c = None
        self.add_sort_column_c = None
        self.info_type_c = None
        self.common_prefix_len_c = None
        self.is_not_running_as_unittest_c = False

    def gen_params(self):
        if self.is_printing_help_c:
            return " -h "
        else:
            args = " "
            for key in filter(lambda aname: aname.endswith('_c') and aname != 'is_printing_help_c', dir(self)):
                val = self.__getattribute__(key)
                # MyLogger.info("config object %s %s", key, val)
                if key.startswith('is'):
                    if val is True:
                        args = args + Config.config_map_dict[key]
                else:
                    if val is not None:
                        args = args + Config.config_map_dict[key]
                        args = args + " " + str(val) + " "
            return args


if __name__ == '__main__':
    conf = Config()
    conf.is_printing_help_c = True
    print conf.gen_params()
