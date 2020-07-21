# MANDATORY -------------------------------------------------------------------- 
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
import os
import SharkNews_etl2
import SharkNews_etl4
import SharkNews_etl1
import SharkNews_etl3

from lib_util import get_filename
from socket import gethostname
from lib_util import utilities
from lib_util import lib_log
import sys
import re
import codecs
from datetime import *
import pandas as pd
from batch_lib import *

# ------------------------------------------------------------------------------

logger = lib_log.Logger()


@init
def section_init():
    """
        **Documentation**
            SharkNews
            Feed Category: Outgoing
            Feed Description: This feed generates SharkNews xml files and delivers to FTP
            Content Set: Shark
            Archive Days: 10
            Dependent Feed(s): N/A
            Content Team/Owner: Mariella Payyappilly
            Failure causes specific to this feed: N/A
            Procedure(s) to recover feed: Feed has to be rerun from scratch.
    """
    remove("SharkNews.log")
    remove("SharkNews.dbg")
    log("-----------------------------------------------------------------------------")
    logdate("SharkNews")
    log("-----------------------------------------------------------------------------")
    ENV.define("J", today())
    ENV.define("job_name", "SharkNews")
    ENV.define("root", "\\\\" + gethostname())
    ENV.define("base_directory", os.path.join(root, 'batch', job_name) + "\\")
    ENV.define("archive_dir", os.path.join(root, 'archive', job_name) + "\\")
    ENV.define("archive_log_dir", os.path.join(root, 'Log', job_name) + "\\")
    ENV.define("Input_dir", os.path.join(root, 'in', 'incoming','SharkFeedExtract') + '\\')
    ENV.define("output_dir", os.path.join(root, 'out', job_name) + "\\")
    ENV.define("zip_dir", os.path.join(root, 'zip', job_name) + "\\")
    ENV.define("config_file", os.path.join(base_directory, "software.ini"))
    ENV.define("netclient_config_a", os.path.join(base_directory, 'netclient_config_a.conf'))
    ENV.define("netclient_config_b", os.path.join(base_directory, 'netclient_config_b.conf'))
    ENV.define("put_file", os.path.join(base_directory, 'file.put'))
    ENV.define('netclient_dir', os.path.join(base_directory, 'lib_util\\NetClient.pyz'))
    ENV.define("zip_filename_0", "sharknews_[YYYY][MM][DD]_[HH][TT][SS].zip")
    

@script
def clean():
    log("-----------------------------------------------------------------------------")
    log("STATUS;%time% SharkNews Begin")
    log("-----------------------------------------------------------------------------")
    logdate("Clean the working folders")
    log("-----------------------------------------------------------------------------")
    call("del /S /Q " + output_dir + "*.*")
    call("del /S /Q " + zip_dir + "*.*")
    utilities.clean_archive_dir(days_limit=10, path=archive_dir)
    log("-----------------------------------------------------------------------------")


@wait_success(clean)

def run_SharkNews_etl2():
    try:
        run = SharkNews_etl2.LoadFile(base_directory,
                                                    Input_dir, output_dir)
        #print(SharkNews_etl.company.shape)
        
        run.run_etl()
    except Exception as e:
        dbg('{}'.format(utilities.capture_trace()))
        raise

# @wait_success(run_SharkNews_etl1)

# def run_SharkNews_etl2():
#     try:
#         run = run_SharkNews_etl2.LoadFile(base_directory,
#                                                     Input_dir, output_dir)
#         #print(SharkNews_etl.company.shape)
        
#         run.run_etl()
#     except Exception as e:
#         dbg('{}'.format(utilities.capture_trace()))
#         raise

# @wait_success(run_SharkNews_etl2)



@end
def notification():
    
    pass
