# -*- coding: utf-8 -*-

import sys
import logging
import logging.handlers
import traceback
import os
import errno


#
#
llvl_critical      = logging.CRITICAL
llvl_error         = logging.ERROR
llvl_warning       = logging.WARNING
llvl_info          = logging.INFO
llvl_diagnostic    = logging.DEBUG + 5
llvl_debug         = logging.DEBUG

llnm_critical       = "critical"
llnm_error          = "error"
llnm_warning        = "warning"
llnm_info           = "info"
llnm_diagnostic     = "diagnostic"
llnm_debug          = "debug"

level_name_to_level = {
    llnm_critical:      llvl_critical,
    llnm_error:         llvl_error,
    llnm_warning:       llvl_warning,
    llnm_info:          llvl_info,
    llnm_diagnostic:    llvl_diagnostic,
    llnm_debug:         llvl_debug
}





def initialize(log_path_, llnm_):
    if not os.path.isdir(log_path_):
        os.mkdir(log_path_)

    log_file = log_path_ + '/test-ctlc.txt'

    logging.addLevelName(llvl_diagnostic, "DIAGNOSTIC")    

    


    file_handler = logging.FileHandler(filename=log_file)
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=level_name_to_level [llnm_], 
        format='[%(asctime)s] [%(levelname)-8s] %(message)s', #format='[%(asctime)s] {%(filename)-15s:%(lineno)d} [%(levelname)-8s] %(message)s',
        handlers=handlers,
        datefmt='%d.%m.%Y %H:%M:%S'
    )











    