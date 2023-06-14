# this is an experimantal new feature introduced by Marco and never fully tested/used
# will worry about pylint if and when we decide to use it and look at details
#pylint: skip-file
"""
CopyCat job type plug-in
"""

import os
import re
import math
import shutil
import string
import tempfile
from functools import reduce
from ast import literal_eval
import json
import hashlib
import tarfile
import ast

from FWCore.PythonUtilities.LumiList import LumiList

from ServerUtilities import BOOTSTRAP_CFGFILE_DUMP, getProxiedWebDir, NEW_USER_SANDBOX_EXCLUSIONS
from ServerUtilities import SERVICE_INSTANCES

import CRABClient.Emulator
from CRABClient import __version__
from CRABClient.UserUtilities import curlGetFileFromURL
from CRABClient.ClientUtilities import colors, LOGGERS, getColumn, getJobTypes, DBSURLS
from CRABClient.JobType.UserTarball import UserTarball
from CRABClient.JobType.CMSSWConfig import CMSSWConfig
from CRABClient.JobType._AnalysisNoUpload import _AnalysisNoUpload
from CRABClient.JobType.BasicJobType import BasicJobType
from CRABClient.ClientMapping import getParamDefaultValue
from CRABClient.JobType.LumiMask import getLumiList, getRunList
from CRABClient.ClientUtilities import bootstrapDone, BOOTSTRAP_CFGFILE, BOOTSTRAP_CFGFILE_PKL
from CRABClient.ClientExceptions import ClientException, EnvironmentException, ConfigurationException, CachefileNotFoundException
from CRABClient.Commands.SubCommand import ConfigCommand
from CRABClient.ClientMapping import parametersMapping, getParamDefaultValue
from ServerUtilities import uploadToS3, downloadFromS3



class CopyOfTask(BasicJobType):
    """
    CMSSW job type plug-in
    """
    def initCRABRest(self):
        serverFactory = CRABClient.Emulator.getEmulator('rest')
        serverhost = SERVICE_INSTANCES.get(self.config.General.instance)
        self.crabserver = serverFactory(hostname=serverhost['restHost'], localcert=self.proxyfilename,
                               localkey=self.proxyfilename, retry=2, logger=self.logger,
                               verbose=False, version=__version__, userAgent='CRABClient')
        self.crabserver.setDbInstance(serverhost['dbInstance'])

        serverFactory = CRABClient.Emulator.getEmulator('rest')
        serverhost = SERVICE_INSTANCES.get(self.config.JobType.copyCatInstance)
        self.crabserverCopyOfTask = serverFactory(hostname=serverhost['restHost'], localcert=self.proxyfilename,
                               localkey=self.proxyfilename, retry=2, logger=self.logger,
                               verbose=False, version=__version__, userAgent='CRABClient')
        self.crabserverCopyOfTask.setDbInstance(serverhost['dbInstance'])

    def getTaskDict(self):
        #getting information about the task
        inputlist = {'subresource':'search', 'workflow': self.config.JobType.copyCatTaskname}

        dictret, dummyStatus, dummyReason = self.crabserverCopyOfTask.get(api='task', data=inputlist)

        dictresult = {}
        self.logger.debug(dictret)
        dictresult['username'] = getColumn(dictret, 'tm_username')
        dictresult['cacheurl'] = getColumn(dictret, 'tm_cache_url')
        dictresult['cachefilename'] = getColumn(dictret, 'tm_user_sandbox')
        dictresult['debugfilename'] = getColumn(dictret, 'tm_debug_files')
        dictresult['jobsw'] = getColumn(dictret, 'tm_job_sw')
        dictresult['jobarch'] = getColumn(dictret, 'tm_job_arch')
        tmp = ast.literal_eval(getColumn(dictret, 'tm_split_args'))
        dictresult['runs'] = tmp['runs']
        dictresult['lumis'] = tmp['lumis']

        return dictresult


    def run(self, filecacheurl = None):
        """
        Override run() for JobType
        """

        self.initCRABRest()

        # reupload sandbox with new hash (from sandbox filename)
        taskInfoDict = self.getTaskDict()
        newCachefilename = f"{hashlib.sha256(taskInfoDict['cachefilename'].encode('utf-8')).hexdigest()}.tar.gz"
        localPathCachefilename = os.path.join(self.workdir, newCachefilename)
        downloadFromS3(crabserver=self.crabserverCopyOfTask, username=taskInfoDict['username'], objecttype='sandbox', logger=self.logger,
                       tarballname=taskInfoDict['cachefilename'], filepath=localPathCachefilename)
        uploadToS3(crabserver=self.crabserver, objecttype='sandbox', filepath=localPathCachefilename,
                   tarballname=newCachefilename, logger=self.logger)

        newDebugfilename = f"{hashlib.sha256(taskInfoDict['debugfilename'].encode('utf-8')).hexdigest()}.tar.gz"
        localPathDebugfilename = os.path.join(self.workdir, newDebugfilename)
        downloadFromS3(crabserver=self.crabserverCopyOfTask, username=taskInfoDict['username'], objecttype='sandbox', logger=self.logger,
                       tarballname=taskInfoDict['debugfilename'], filepath=taskInfoDict['debugfilename'])

        tar = tarfile.open(taskInfoDict['debugfilename'], mode='r')
        tar.extractall(path=os.path.join(self.workdir, "CopyOfTask"))

        #import pdb; pdb.set_trace()

        copyOfTaskCrabConfig = os.path.join(self.workdir, 'CopyOfTask/debug/crabConfig.py')
        copyOfTaskPSet = os.path.join(self.workdir, 'CopyOfTask/debug/originalPSet.py')
        debugFilesUploadResult = None
        with UserTarball(name=localPathDebugfilename, logger=self.logger, config=self.config,
                         crabserver=self.crabserver, s3tester=self.s3tester) as dtb:

            dtb.addMonFilesCopyOfTask(copyOfTaskCrabConfig, copyOfTaskPSet)
            try:
                debugFilesUploadResult = dtb.upload(filecacheurl = filecacheurl)
            except Exception as e:
                msg = ("Problem uploading debug_files.tar.gz.\nError message: %s.\n"
                       "More details can be found in %s" % (e, self.logger.logfile))
                LOGGERS['CRAB3'].exception(msg) #the traceback is only printed into the logfile

        #import pdb; pdb.set_trace()

        # parse config (copy from submit subcommand)
        cfgcmd = ConfigCommand()
        cfgcmd.logger = self.logger
        cfgcmd.loadConfig(copyOfTaskCrabConfig)
        #cfgcmd.loadConfig('crabConfig2.py')
        cfgcmd.configuration.JobType.psetName = copyOfTaskPSet
        # remove lumimasks here to prevent loading lumis form file
        cfgcmd.configuration.Data.lumiMask = None
        cfgcmd.configuration.Data.runRange = None

        configreq = {'dryrun': 0}
        for param in parametersMapping['on-server']:
            #mustbetype = getattr(types, parametersMapping['on-server'][param]['type'])
            default = parametersMapping['on-server'][param]['default']
            config_params = parametersMapping['on-server'][param]['config']
            for config_param in config_params:
                attrs = config_param.split('.')
                temp = cfgcmd.configuration
                for attr in attrs:
                    temp = getattr(temp, attr, None)
                    if temp is None:
                        break
                if temp is not None:
                    configreq[param] = temp
                    break
                elif default is not None:
                    configreq[param] = default
                    temp = default
                else:
                    ## Parameter not strictly required.
                    pass
            ## Check that the requestname is of the right type.
            ## This is not checked in SubCommand.validateConfig().
            #if param == 'workflow':
                #if isinstance(self.requestname, mustbetype):
            #    configreq['workflow'] = .requestname
            ## Translate boolean flags into integers.
            if param in ['savelogsflag', 'publication', 'nonprodsw', 'useparent',\
                           'ignorelocality', 'saveoutput', 'oneEventMode', 'nonvaliddata', 'ignoreglobalblacklist',\
                           'partialdataset', 'requireaccelerator']:
                configreq[param] = 1 if temp else 0
            ## Translate DBS URL aliases into DBS URLs.
            elif param in ['dbsurl', 'publishdbsurl']:
                if param == 'dbsurl':
                    dbstype = 'reader'
                elif param == 'publishdbsurl':
                    dbstype = 'writer'
                allowed_dbsurls = DBSURLS[dbstype].values()
                allowed_dbsurls_aliases = DBSURLS[dbstype].keys()
                if configreq[param] in allowed_dbsurls_aliases:
                    configreq[param] = DBSURLS[dbstype][configreq[param]]
                elif configreq[param].rstrip('/') in allowed_dbsurls:
                    configreq[param] = configreq[param].rstrip('/')
            elif param == 'scriptexe' and 'scriptexe' in configreq:
                configreq[param] = os.path.basename(configreq[param])
            elif param in ['acceleratorparams'] and param in configreq:
                configreq[param] = json.dumps(configreq[param])


        #import pdb; pdb.set_trace()

        # parsing Analysis jobtype
        pluginParams = [cfgcmd.configuration, self.proxyfilename, self.logger,
                        self.workdir, self.crabserver, self.s3tester]
        plugjobtype = _AnalysisNoUpload(*pluginParams)
        dummy_inputfiles, jobconfig = plugjobtype.run(filecacheurl)
        #import pdb; pdb.set_trace()

        configreq.update(jobconfig)

        #import pdb; pdb.set_trace()

        # pop
        configreq.pop('workflow', None)
        configreq.pop('publication', None)
        configreq.pop('outLFNDirBase', None)
        configreq.pop('outputDatasetTag', None)
        configreq.pop('asyncdest', None)
        # replace
        configreq['jobsw'] = taskInfoDict['jobsw']
        configreq['jobarch'] = taskInfoDict['jobarch']
        configreq['runs'] = taskInfoDict['runs']
        configreq['lumis'] = taskInfoDict['lumis']

        # new filename
        configreq['cachefilename'] = newCachefilename
        configreq['debugfilename'] = newDebugfilename
        configreq['debugfilename'] = "%s.tar.gz" % debugFilesUploadResult
        configreq['cacheurl'] = filecacheurl

        #import pdb; pdb.set_trace()
        return '', configreq


    def validateConfig(self, config):
        """
        """
        # skip it all for now
        valid, reason = self.validateBasicConfig(config)
        if not valid:
            return valid, reason

        return True, "Valid configuration"

    def validateBasicConfig(self, config):
        """

        """
        return True, "Valid configuration"
