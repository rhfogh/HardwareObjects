#! /usr/bin/env python
# encoding: utf-8
"""Global phasing workflow runner
"""

__copyright__ = """
  * Copyright © 2016 - 2017 by Global Phasing Ltd.
"""
__author__ = "rhfogh"
__date__ = "06/04/17"

import os
import gevent
import uuid
import logging
import time
import pprint
import queue_model_objects_v1 as queue_model_objects
from HardwareRepository.HardwareRepository import dispatcher
from HardwareRepository.BaseHardwareObjects import HardwareObject


class State(object):
    """
    Class for mimic the PyTango state object - copied from EdnaWorkflow
    """

    def __init__(self, parent):
        self._value = "ON"
        self._parent = parent

    def getValue(self):
        return self._value

    def setValue(self, newValue):
        self._value = newValue
        self._parent.state_changed(newValue)

    def delValue(self):
        pass

    value = property(getValue, setValue, delValue, "Property for value")


class GphlWorkflow(HardwareObject):
    """Global Phasing workflow runner.
    Mimics the interface of EdnaWorkflow (including state handling) to fit
    into the same Queue entry.
    Internal functioning is totally different from EdnaWorkflow

    """
    # TODO - consider changing interface to get away from Tango emulation

    # Imported here to keep it out of the shared top namespace
    # NB, by the time the code gets here, HardwareObjects is on the PYTHONPATH
    # as is HardwareRepository
    import GphlMessages

    # TODO the below is copied directly from EdnaWorkflow. Must be reviewed.

    def __init__(self, name):
        HardwareObject.__init__(self, name)
        self._state = State(self)
        self._command_failed = False
        # self._besWorkflowId = None
        self._gevent_event = None
        # self._bes_host = None
        # self._bes_port = None

        # Added for GPhL
        self.workflow_name = None
        self.workflow_server_hwobj = None
        self.queue_entry = None
        self._gphl_process_finished = None
        self._server_subprocess_names = {}

    def _init(self):
        pass

    def init(self):
        self._session_object = self.getObjectByRole("session")
        self._gevent_event = gevent.event.Event()
        # self._bes_host = self.getProperty("bes_host")
        # self._bes_port = int(self.getProperty("bes_port"))
        self.state.value = "ON"


    def getState(self):
        return self._state

    def setState(self, newState):
        self._state = newState

    def delState(self):
        pass

    state = property(getState, setState, delState, "Property for state")


    def command_failure(self):
        return self._command_failed

    def set_command_failed(self, *args):
        logging.getLogger("HWR").error("Workflow '%s' Tango command failed!" % args[1])
        self._command_failed = True

    def state_changed(self, new_value):
        new_value = str(new_value)
        logging.getLogger("HWR").debug('%s: state changed to %r',
                                       str(self.name()), new_value)
        self.emit('stateChanged', (new_value, ))

    def workflow_end(self):
        """
        The workflow has finished, sets the state to 'ON'
        """
        # If necessary unblock dialog
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        self.state.value = "ON"

    def open_dialog(self, dict_dialog):
        # If necessary unblock dialog
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        self.params_dict = dict()
        if "reviewData" in dict_dialog and "inputMap" in dict_dialog:
            review_data = dict_dialog["reviewData"]
            for dictEntry in dict_dialog["inputMap"]:
                if "value" in dictEntry:
                    value = dictEntry["value"]
                else:
                    value = dictEntry["defaultValue"]
                self.params_dict[dictEntry["variableName"]] = str(value)
            self.emit('parametersNeeded', (review_data, ))
            self.state.value = "OPEN"
            self._gevent_event.clear()
            while not self._gevent_event.is_set():
                self._gevent_event.wait()
                time.sleep(0.1)
        return self.params_dict

    def get_values_map(self):
        return self.params_dict

    def set_values_map(self, params):
        self.params_dict = params
        self._gevent_event.set()

    def get_available_workflows(self):
        workflow_list = list()
        no_wf = len( self['workflow'] )
        for wf_i in range( no_wf ):
            wf = self['workflow'][wf_i]
            dict_workflow = dict()
            dict_workflow["name"] = str(wf.title)
            dict_workflow["path"] = str(wf.path)
            dict_workflow["requires"] = wf.getProperty('requires')
            dict_workflow["doc"] = ""
            workflow_list.append(dict_workflow)
        return workflow_list

    def abort(self):
        logging.getLogger("HWR").info('Aborting current workflow')
        # If necessary unblock dialog
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        self._command_failed = False

        # if self._besWorkflowId is not None:
        #     abortWorkflowURL = os.path.join("/BES", "bridge", "rest", "processes", self._besWorkflowId, "STOP?timeOut=0")
        #     logging.info("BES web service URL: %r" % abortWorkflowURL)
        #     conn = httplib.HTTPConnection(self._bes_host, self._bes_port)
        #     conn.request("POST", abortWorkflowURL)
        #     response = conn.getresponse()
        #     if response.status == 200:
        #         workflowStatus=response.read()
        #         logging.info("BES {0}: {1}".format(self._besWorkflowId, workflowStatus))

        dispatcher.send(
            self.GphlMessages.message_type_to_signal['BeamlineAbort'], self,
            message="GPhL workflow run aborted from GphlWorkflow HardwareObject"
        )

        self.state.value = "ON"



    # # INFO: These are the listArguments
    # model.set_name("Workflow task")
    # model.set_type(params["wfname"])
    #
    # beamline_params = {}
    # beamline_params['directory'] = model.path_template.directory
    # beamline_params['prefix'] = model.path_template.get_prefix()
    # beamline_params['run_number'] = model.path_template.run_number
    # beamline_params['collection_software'] = 'MXCuBE - 3.0'
    # beamline_params['sample_node_id'] = sample_model._node_id
    # beamline_params['sample_lims_id'] = sample_model.lims_id
    #
    # params_list = map(str, list(itertools.chain(*beamline_params.iteritems())))
    # params_list.insert(0, params["wfpath"])
    # params_list.insert(0, 'modelpath')
    #
    # model.params_list = params_list
    #
    # model.set_enabled(task_data['checked'])
    # entry.set_enabled(task_data['checked'])


    def start(self, listArguments):

        # Copy fronm EdnaWorkflow
        # TODO reconsider

        # If necessary unblock dialog
        if not self._gevent_event.is_set():
            self._gevent_event.set()
        self.state.value = "RUNNING"

        self.dictParameters = {}
        iIndex = 0
        if (len(listArguments) == 0):
            self.error_stream("ERROR! No input arguments!")
            return
        elif (len(listArguments) % 2 != 0):
            self.error_stream("ERROR! Odd number of input arguments!")
            return
        while iIndex < len(listArguments):
            self.dictParameters[listArguments[iIndex]] = listArguments[iIndex+1]
            iIndex += 2
        logging.info("Input arguments:")
        logging.info(pprint.pformat(self.dictParameters))

        if "modelpath" in self.dictParameters:
            modelPath = self.dictParameters["modelpath"]
            if "." in modelPath:
                modelPath = modelPath.split(".")[0]
            self.workflow_name = os.path.basename(modelPath)
        else:
            self.error_stream("ERROR! No modelpath in input arguments!")
            return

        # time0 = time.time()
        # self.startBESWorkflow()
        # time1 = time.time()
        # logging.info("Time to start workflow: {0}".format(time1-time0))

        # End Copy of EdnaWorkflow

        # Start GPhL workflow handling
        try:
            self._gphl_process_finished = gevent.event.AsyncResult()

            # Fork off workflow server process
            self.workflow_server_hwobj.start_workflow(self,
                self.queue_entry.get_data_model().workflow_type
            )

            # Wait for workflow execution to finish
            # Queue child entries are set up and triggered through dispatcher
            final_message = self._gphl_process_finished.get(
                timeout=self.workflow_server_hwobj.execution_timeout
            )
            if final_message is None:
                final_message = 'Timeout'
            self.echo_info(final_message)
        finally:
            self.cleanup_workflow_execution()


    # def startBESWorkflow(self):
    #
    #     logging.info("Starting workflow {0}".format(self.workflowName))
    #     logging.info("Starting a workflow on http://%s:%d/BES" % (self._bes_host, self._bes_port))
    #     startWorkflowURL = os.path.join("/BES", "bridge", "rest", "processes", self.workflowName, "RUN")
    #     isFirstParameter = True
    #     self.dictParameters["initiator"] = self._session_object.endstation_name
    #     self.dictParameters["externalRef"] = self._session_object.get_proposal()
    #     # Build the URL
    #     for key in self.dictParameters:
    #         urlParameter = "%s=%s" % (key, self.dictParameters[key].replace(" ", "_"))
    #         if isFirstParameter:
    #             startWorkflowURL += "?%s" % urlParameter
    #         else:
    #             startWorkflowURL += "&%s" % urlParameter
    #         isFirstParameter = False
    #     logging.info("BES web service URL: %r" % startWorkflowURL)
    #     conn = httplib.HTTPConnection(self._bes_host, self._bes_port)
    #     headers = {"Accept": "text/plain"}
    #     conn.request("POST", startWorkflowURL, headers=headers)
    #     response = conn.getresponse()
    #     if response.status == 200:
    #         self.state.value = "RUNNING"
    #         requestId=response.read()
    #         logging.info("Workflow started, request id: %r" % requestId)
    #         self._besWorkflowId = requestId
    #     else:
    #         logging.error("Workflow didn't start!")
    #         requestId = None
    #         self.state.value = "ON"



    #
    # def __init__(self, view=None, data_model=None,
    #              view_set_queue_entry=True):
    #     BaseQueueEntry.__init__(self, view, data_model, view_set_queue_entry)
    #     self.workflow_hwobj = None
    #     self._gphl_process_finished = None
    #     self._server_subprocess_names = {}

    # def execute(self):
    #     BaseQueueEntry.execute(self)
    #     self._gphl_process_finished = gevent.event.AsyncResult()
    #
    #     # Fork off workflow server process
    #     self.workflow_hwobj.start_workflow(self,
    #                                        self.get_data_model().workflow_type)
    #
    #     # Wait for workflow execution to finish
    #     # Queue child entries are set up and triggered through dispatcher
    #     final_message = self._gphl_process_finished.get(
    #         timeout=self.workflow_hwobj.execution_timeout
    #     )
    #     if final_message is None:
    #         final_message = 'Timeout'
    #     self.echo_info(final_message)

    def setup_workflow_execution(self, queue_entry):
        workflow_server_hwobj = self.beamline_setup.gphl_workflow_server
        self.workflow_server_hwobj = workflow_server_hwobj
        self.queue_entry = queue_entry

        # Set up local listeners
        dispatcher.connect(self.echo_info_string,
                           'GPHL_INFO',
                           workflow_server_hwobj)
        dispatcher.connect(self.echo_subprocess_started,
                           'GPHL_SUBPROCESS_STARTED',
                           workflow_server_hwobj)
        dispatcher.connect(self.echo_subprocess_stopped,
                           'GPHL_SUBPROCESS_STOPPED',
                           workflow_server_hwobj)
        dispatcher.connect(self.get_configuration_data,
                           'GPHL_REQUEST_CONFIGURATION',
                           workflow_server_hwobj)
        dispatcher.connect(self.setup_data_collection,
                           'GPHL_GEOMETRIC_STRATEGY',
                           workflow_server_hwobj)
        dispatcher.connect(self.collect_data,
                           'GPHL_COLLECTION_PROPOSAL',
                           workflow_server_hwobj)
        dispatcher.connect(self.select_lattice,
                           'GPHL_CHOOSE_LATTICE',
                           workflow_server_hwobj)
        dispatcher.connect(self.centre_sample,
                           'GPHL_REQUEST_CENTRING',
                           workflow_server_hwobj)
        dispatcher.connect(self.obtain_prior_information,
                           'GPHL_OBTAIN_PRIOR_INFORMATION',
                           workflow_server_hwobj)
        dispatcher.connect(self.prepare_for_centring,
                           'GPHL_PREPARE_FOR_CENTRING',
                           workflow_server_hwobj)
        dispatcher.connect(self.workflow_aborted,
                           'GPHL_WORKFLOW_ABORTED',
                           workflow_server_hwobj)
        dispatcher.connect(self.workflow_completed,
                           'GPHL_WORKFLOW_COMPLETED',
                           workflow_server_hwobj)
        dispatcher.connect(self.workflow_failed,
                           'GPHL_WORKFLOW_FAILED',
                           workflow_server_hwobj)


    def cleanup_workflow_execution(self):

        # May not be necessary but it does not hurt
        dispatcher.disconnect(sender=self.workflow_server_hwobj)

    def echo_info_string(self, info, correlation_id):
        """Print text info to console,. log etc."""
        # TODO implement properly
        subprocess_name = self._server_subprocess_names.get(correlation_id)
        if subprocess_name:
            logging.info ('%s: %s' % (subprocess_name, info))
        else:
            logging.info(info)

    def echo_subprocess_started(self, subprocess_started, correlation_id):
        name =subprocess_started.name
        if correlation_id:
            self._server_subprocess_names[name] = correlation_id
        logging.info('%s : STARTING' % name)

    def echo_subprocess_stopped(self, subprocess_stopped, correlation_id):
        name =subprocess_stopped.name
        if correlation_id in self._server_subprocess_names:
            del self._server_subprocess_names[name]
        logging.info('%s : FINISHED' % name)

    def get_configuration_data(self, request_configuration,
                               correlation_id):
        data_location = self.getProperty('beamline_configuration_directory')
        return self.GphlMessages.ConfigurationData(data_location)

    def setup_data_collection(self, geometric_strategy, correlation_id):
        pass
        raise NotImplementedError()

        ## Display GeometricStrategy, with RotationSetting ID.

        ## Query imageWidth, transmission, exposure and wedgeWidth
        ## depending on values for userModifiable and isInterleaved.

        ## Create SampleCentred object and set user entered values

        # NBNB psdeudocode
        goniostatRotationIds = set()
        for sweep in geometric_strategy.sweeps:
            setting = sweep.goniostatSweepSetting
            if setting.ID not in goniostatRotationIds:
                goniostatRotationIds.add(setting.ID)
                ## Rotate sample to setting
                ## Optionally translate to attached translation setting
                ## Query user for alternative rotation
                ## If alternative rotation create new setting object
                ## and rotate to new setting
                ## Trigger centring dialogue
                ## If translation or rotation setting is changed
                ## (at first: ALWAYS) then:
                ##   Create GoniostatTranslation
                ##   and add it to SampleCentred.goniostatTranslations

        ## Return SampleCentred


    def collect_data(self, collection_proposal, correlation_id):
        pass

        ## Display collection proposal in suitable form
        ## Query  relativeImageDir,
        ## and ask for go/nogo decision

        # NBNB pseudocode
        for scan in collection_proposal.scans:
            pass
            ## rotate to scan.sweep.goniostatSweepSetting position
            ## and translate to corresponding translation position

            ## Set beam, detector and beamstop
            ## set up acquisition and acquire

            ## NB the entire sequence can be put on the queue at once
            ## provided the motor movements can be  queued.

        ## return collectionDone
        raise NotImplementedError()

    def select_lattice(self, choose_lattice, correlation_id):
        pass
        raise NotImplementedError()

        ## Display solution and query user for lattice

        ## Create SelectedLattice and return it

    def centre_sample(self, request_centring, correlation_id):

        logging.info ('Start centring no. %s of %s'
                      % (request_centring.currentSettingNo,
                         request_centring.totalRotations))

        ## Rotate sample to RotationSetting
        goniostatRotation = request_centring.goniostatRotation
        axisSettings = goniostatRotation.axisSettings

        # NBNB it is up to beamline setup etc. to ensure that the
        # axis names are correct - and this is what SampleCentring uses
        name = 'GPhL_centring_%s' % request_centring.currentSettingNo
        sc_model = queue_model_objects.SampleCentring(
            name=name, kappa=axisSettings['kappa'],
            kappa_phi=axisSettings['kappa_phi']
        )
        # PROBLEM 1 - How do you get from here to a SampleCentring queue item?
        # a.k.a: Why is SampleCentringQueueItem not instantiated anywhere?
        # PROBLEM 2 - how do you put omega positioning on the queue?


        diffractometer = self.getObjectByRole("diffractometer")
        positionsDict = diffractometer.getPositions()
        # # TODO check that axis names match beamline, or translate them
        # diffractometer.moveMotors(axisSettings)


        ## Trigger centring dialogue

        ## When done get translation setting

        ## Create GoniostatTranslation and return CentringDone

        raise NotImplementedError()

    def prepare_for_centring(self, gphl_message, correlation_id):

        raise NotImplementedError()

        return self.GphlMessages.ReadyForCentring()

    def obtain_prior_information(self, gphl_message, correlation_id):

        sample_node_id = self.dictParameters.get('sample_node_id')
        queue_model = self.getObjectByRole("QueueModel")
        sample_model = queue_model.get_node(sample_node_id)

        crystals = sample_model.crystals
        if crystals:
            crystal = crystals[0]

            unitCell = self.GphlMessages.UnitCell(
                crystal.cell_a, crystal.cell_b, crystal.cell_c,
                crystal.cell_alpha, crystal.cell_beta, crystal.cell_gamma,
            )
            space_group = crystal.space_group
        else:
            unitCell = space_group = None

        userProvidedInfo = self.GphlMessages.UserProvidedInfo(
            scatterers=(),
            lattice=None,
            spaceGroup=space_group,
            cell=unitCell,
            expectedResolution=None,
            isAnisotropic=None,
            phasingWavelengths=()
        )
        # NB scatterers, lattice, isAnisotropic, phasingWavelengths,
        # and expectedResolution are
        # not obviously findable and would likely have to be set explicitly
        # in UI. Meanwhile leave them empty

        # Look for existing uuid
        for text in sample_model.lims_code, sample_model.code, sample_model.name:
            if text:
                try:
                    existing_uuid = uuid.UUID(text)
                except:
                    # The error expected if this goes wrong is ValueError.
                    # But whatever the error we want to continue
                    pass
                else:
                    # Text was a valid uuid string. Use the uuid.
                    break
        else:
            existing_uuid = None

        # TODO check if this is correct
        rootDirectory = self.path_template.get_archive_directory()

        priorInformation = self.GphlMessages.PriorInformation(
            sampleId=existing_uuid or uuid.uuid1(),
            sampleName=(sample_model.name or sample_model.code
                        or sample_model.lims_code),
            rootDirectory=rootDirectory,
            userProvidedInfo=userProvidedInfo
        )
        #
        return priorInformation,

    def workflow_aborted(self, message_type, workflow_aborted):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)

    def workflow_completed(self, message_type, workflow_completed):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)

    def workflow_failed(self, message_type, workflow_failed):
        # NB Echo additional content later
        self._gphl_process_finished.set(message_type)
