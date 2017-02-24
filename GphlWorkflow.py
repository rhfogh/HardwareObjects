import uuid
import collections
from gevent.event import AsyncResult
from py4j import clientserver
from HardwareRepository.dispatcher import dispatcher
from HardwareRepository.BaseHardwareObjects import HardwareObject
from HardwareObjects import GphlMessages



def int2Float(value):
    """Convert int to float"""
    if isinstance(value, int):
        return float(value)
    else:
        return value

class GphlWorkflow(HardwareObject, object):
    """
    This HO acts as a gateway to the Global Phasing workflow engine.
    """

    # object states
    INITIALISING = 'INITIALISING'
    ACTIVE = 'ACTIVE'
    CONNECTING = 'CONNECTING'
    LISTENING = 'LISTENING'
    RESPONDING = 'RESPONDING'
    
    def __init__(self, name):
        HardwareObject.__init__(self, name)

        # Py4J gateway to external workflow program
        self._gateway = None

        # ID for current workflow calculation
        self._enactment_id = None

        # Name of workflow being executed.
        self._workflow_name = None

        # AsyncResult object waiting for a response.
        self._async_result = None

        
    def _init(self):
        pass

    def init(self):

        # NB, these do not match a message - we may change that later
        dispatcher.connect(self.start_workflow, 'GPHL_START_WORKFLOW')
        dispatcher.connect(self.shutdown_workflow_server,
                           'GPHL_SHUTDOWN_WORKFLOW')
        dispatcher.connect(self.abort_workflow, 'GPHL_ABORT_WORKFLOW')

        # NB These should eventually be set in standard configuration xml files
        pythonParameters = {}
        javaParameters = {'auto_convert':True}

        self._gateway = clientserver.ClientServer(
            java_parameters=clientserver.JavaParameters(**javaParameters),
            python_parameters=clientserver.PythonParameters(**pythonParameters),
            python_server_entry_point=self)

    @property
    def state(self):
        """Execution state of GPhL Workflow"""
        if self._gateway is None:
            return self.INITIALISING
        elif self._enactment_id is None:
            if self._workflow_name is None:
                return self.ACTIVE
            else:
                return self.CONNECTING
        else:
            if self._async_result is None:
                return self.LISTENING
            else:
                return self.RESPONDING

    @property
    def workflow_name(self):
        """Name of currently executing workflow"""
        return self._workflow_name

    def start_workflow(self, controller, workflow_name):

        if self.state != 'ACTIVE':
            raise RuntimeError("Workflow is already running, cannot be started")
        
        # Controller listeners are set up on the other side
        # before this is called

        # These are our local listeners. They will be removed
        # automatically when the controller is deleted.
        dispatcher.connect(self.send_configuration_data,
                           'GPHL_CONFIGURATION_DATA',
                           controller)
        dispatcher.connect(self.send_sample_centred,
                           'GPHL_SAMPLE_CENTRED',
                           controller)
        dispatcher.connect(self.send_collection_done,
                           'GPHL_COLLECTION_DONE',
                           controller)
        dispatcher.connect(self.send_selected_lattice,
                           'GPHL_SELECTED_LATTICE',
                           controller)
        dispatcher.connect(self.send_centring_done,
                           'GPHL_CENTRING_DONE',
                           controller)
        dispatcher.connect(self.send_data_acquisition_start,
                           'GPHL_DATA_ACQUISITION_START',
                           controller)

        self._workflow_name = workflow_name
        # Here we make and send the workflow start-run message
        # NB currently done under 'wfrun' alias
        raise NotImplementedError()

    def shutdown_workflow_server(self, message=None):
        """Shut down external workflow server.
        Currently the same as abort_workflow"""
        self.abort_workflow(message=message)

    def abort_workflow(self, message=None, payload=None):
        """Abort workflow - may be called from controller in any state"""

        if self.state in (self.INITIALISING, self.ACTIVE):
            raise RuntimeError("Workflow is not running, cannot be aborted")

        # NB signals will have no effect if controller is already deleted.
        if payload is None:
            payload = GphlMessages.WorkflowAborted()
        dispatcher.send(GphlMessages.message_type_to_signal['String'],
                        self, payload=message or payload.__class__.__name__)
        dispatcher.send(GphlMessages.message_type_to_signal['WorkflowAborted'],
                        self, payload=GphlMessages.WorkflowAborted())
        self._reset()
        # Must also send message to server (not currently possible)

    def _reset(self):
        """Reset to ACTIVE state"""

        self._enactment_id = None
        self._workflow_name = None
        async_result = self._async_result
        if async_result is not None:
            self._async_result = None
            # Set async_result to avoid processes waiting for it
            async_result.set('RESET!!!')


    def get_available_workflows(self):

        available_workflows = (
            ('Translation_Calibration', 'Goniostat translational calibration'),
            ('Data_Acquisition', 'Optimal data acquisition')
        )
        result =  collections.OrderedDict(available_workflows)
        #
        return result


    def _receive_from_server(self, py4jMessage):
        """Receive and process message from workflow server
        Return goes to server"""
        xx = py4jMessage.getEnactmentId()
        enactment_id = xx and xx.toString()

        xx = py4jMessage.getCorrelationId()
        correlation_id = xx and xx.toString()

        message_type, payload = self._decode_py4j_message(py4jMessage)

        # ALso serves to trigger abort at end of function
        abort_message = None

        if not payload:
            abort_message = ("Payload could not be decoded for message %s"
                             % message_type)

        elif not enactment_id:
            abort_message = "Received message with empty enactment_id"

        else:
            # Set send_signal and enactment_id, testing for errors
            try:
                send_signal = GphlMessages.message_type_to_signal[message_type]
            except KeyError:
                abort_message = ("Unknown message type from server: %s"
                                 % message_type)
            else:

                if self._enactment_id is None:
                    # NB this should be made less primitive
                    # once we are past direct function calls
                    self._enactment_id = enactment_id
                elif self._enactment_id != enactment_id:
                    abort_message = (
                        "Workflow process id %s != message process id %s"
                        % (self._enactment_id, enactment_id)
                    )

        if not abort_message:

            if message_type in ('String',
                                'SubprocessStarted',
                                'SubprocessStopped'):
                # INFO messages to echo - no response to server needed
                responses = dispatcher.send(send_signal, self, payload=payload,
                                            correlation_id=correlation_id)
                if not responses:
                    abort_message = ("No response to %s info message"
                                     % message_type)

            elif message_type in ('RequestConfiguration',
                                  'GeometricStrategy',
                                  'CollectionProposal',
                                  'ChooseLattice',
                                  'RequestCentring' ):
                # Requests:
                self._async_result = AsyncResult()
                responses = dispatcher.send(send_signal, self, payload=payload,
                                            correlation_id=correlation_id)
                if not responses:
                    abort_message = "No response to %s request" % message_type

            elif message_type == 'WorkflowReady':

                # Must send back either PriorInformation or StartCalibration???

                self._enactment_id = enactment_id
                if self._workflow_name == 'Translation_Calibration':
                    # No rwesponse - next message comes from server
                    # NBNB this is temporary
                    return
                elif self._workflow_name == 'Data_Acquisition':
                    # Treated as a request for priorInformation
                    self._async_result = AsyncResult()
                    responses = dispatcher.send(send_signal, self,
                                                payload=payload,
                                                correlation_id=correlation_id)
                    if not responses:
                        abort_message = ("No response to %s message for %s"
                                         % (message_type, self._workflow_name))

            elif message_type in ('WorkflowAborted',
                                  'WorkflowCompleted',
                                  'WorkflowFailed'):
                self.abort_workflow(message=message_type, payload=payload)
                # NB we cannot send abort messages to server,
                # so we return a dummy to raise an error
                return 'Abort1!!!'

            else:
                abort_message = ("Unknown message type: %s" % message_type)

        if abort_message:
            self.abort_workflow(message=abort_message)
            # Abort messages not properly implemented yet.
            # This will shut server down with error
            return 'Abort2!!!'

        elif self._async_result:
            result = self._async_result.get()
            self._async_result = None
            if result.getCollelationId().toString() != correlation_id:
                self.abort_workflow(
                    message="Mismatched correlation_id for response to %s"
                    % message_type
                )
                # Abort messages not properly implemented yet.
                # This will shut server down with error
                return 'Abort3!!!'
            else:
                return result

        else:
            # No response expected
            return

    # NBNB TODO temporary fix - remove when Java calls have been renamed
    msgToBcs = _receive_from_server


    #Conversion to Python

    def _decode_py4j_message(self, message):
        """Extract messageType and convert py4J object to python object"""

        # Determine message type
        messageType = message.getPayloadClass().getSimpleName()
        if messageType.endswith('Impl'):
            messageType = messageType[:-4]
        converterName = '_%s_to_python' % messageType

        if self.debug:
            print ('@~@~ processMessage', messageType, converterName,
                   hasattr(self, converterName))

        try:
            # determine converter function
            converter = getattr(self, converterName)
        except AttributeError:
            print ("Message type %s not recognised (no %s function)"
                   % (messageType, converterName))
            result = None
        else:
            try:
                # Convert to Python objects
                result = converter(message.getPayload())
            except NotImplementedError:
                print('Processing of message %s not implemented' % messageType)
                result = None
        #
        return messageType, result

    def _RequestConfiguration_to_python(self, py4jRequestConfiguration):
        return GphlMessages.RequestConfiguration()

    def _WorkflowReady_to_python(self, py4jWorkflowReady):
        return GphlMessages.WorkflowReady()

    def _GeometricStrategy_to_python(self, py4jGeometricStrategy):
        uuidString = py4jGeometricStrategy.getId().toString()
        sweeps = frozenset(self._Sweep_to_python(x)
                           for x in py4jGeometricStrategy.getSweeps()
                           )
        return GphlMessages.GeometricStrategy(
            isInterleaved=py4jGeometricStrategy.isInterleaved(),
            isUserModifiable=py4jGeometricStrategy.isUserModifiable(),
            allowedWidths=py4jGeometricStrategy.getAllowedWidths(),
            defaultWidthIdx=py4jGeometricStrategy.getDefaultWidthIdx(),
            sweeps=sweeps,
            id=uuid.UUID(uuidString)
        )

    def _SubprocessStarted_to_python(self, py4jSubprocessStarted):
        return GphlMessages.SubprocessStarted(
            name=py4jSubprocessStarted.getName()
        )

    def _SubprocessStopped_to_python(self, py4jSubprocessStopped):
        return GphlMessages.SubprocessStopped()

    def _ChooseLattice_to_python(self, py4jChooseLattice):
        format = py4jChooseLattice.getFormat().toString()
        solutions = py4jChooseLattice.getSolutions()
        lattices = py4jChooseLattice.getLattice()
        return GphlMessages.ChooseLattice(format=format, solutions=solutions,
                                          lattices=lattices)

    def _CollectionProposal_to_python(self, py4jCollectionProposal):
        uuidString = py4jCollectionProposal.getId().toString()
        strategy = self._GeometricStrategy_to_python(
            py4jCollectionProposal.getStrategy()
        )
        id2Sweep = dict((str(x.id),x) for x in strategy.sweeps)
        scans = []
        for py4jScan in py4jCollectionProposal.getScans():
            sweep = id2Sweep[py4jScan.getSweep().getId().toString()]
            scans.append(self._Scan_to_python(py4jScan, sweep))
        return GphlMessages.CollectionProposal(
            relativeImageDir=py4jCollectionProposal.getRelativeImageDir(),
            strategy=strategy,
            scans=scans,
            id=uuid.UUID(uuidString)
        )


    def __WorkflowDone_to_python(self, py4jWorkflowDone, cls):
        Issue = GphlMessages.Issue
        issues = []
        for py4jIssue in py4jWorkflowDone.getIssues():
            component = py4jIssue.getComponent()
            message = py4jIssue.getMessage()
            code = py4jIssue.getCode()
            issues.append(Issue(component=component, message=message,
                                code=code))
        #
        return cls(issues=issues)

    def _WorkflowCompleted_to_python(self, py4jWorkflowCompleted):
        return self.__WorkflowDone_to_python(py4jWorkflowCompleted,
                                             GphlMessages.WorkflowCompleted)

    def _WorkflowAborted_to_python(self, py4jWorkflowAborted):
        return self.__WorkflowDone_to_python(py4jWorkflowAborted,
                                             GphlMessages.WorkflowAborted)

    def _WorkflowFailed_to_python(self, py4jWorkflowFailed):
        return self.__WorkflowDone_to_python(py4jWorkflowFailed,
                                             GphlMessages.WorkflowFailed)

    def _RequestCentring_to_python(self, py4jRequestCentring):
        goniostatRotation = self._GoniostatRotation_to_python(
            py4jRequestCentring.getGoniostatRotation()
        )
        return GphlMessages.RequestCentring(
            currentSettingNo=py4jRequestCentring.getCurrentSettingNo(),
            totalRotations=py4jRequestCentring.getTotalRotations(),
            goniostatRotation=goniostatRotation
        )

    def _GoniostatRotation_to_python(self, py4jGoniostatRotation):
        if py4jGoniostatRotation is None:
            return None
        uuidString = py4jGoniostatRotation.getId().toString()

        axisSettings = py4jGoniostatRotation.getAxisSettings()
        #
        return GphlMessages.GoniostatRotation(id=uuid.UUID(uuidString),
                                          **axisSettings)

    def _BeamstopSetting_to_python(self, py4jBeamstopSetting):
        if py4jBeamstopSetting is None:
            return None
        uuidString = py4jBeamstopSetting.getId().toString()
        axisSettings = py4jBeamstopSetting.getAxisSettings()
        #
        return GphlMessages.BeamstopSetting(id=uuid.UUID(uuidString),
                                        **axisSettings)

    def _DetectorSetting_to_python(self, py4jDetectorSetting):
        if py4jDetectorSetting is None:
            return None
        uuidString = py4jDetectorSetting.getId().toString()
        axisSettings = py4jDetectorSetting.getAxisSettings()
        #
        return GphlMessages.DetectorSetting(id=uuid.UUID(uuidString),
                                        **axisSettings)

    def _BeamSetting_to_python(self, py4jBeamSetting):
        if py4jBeamSetting is None:
            return None
        uuidString = py4jBeamSetting.getId().toString()
        #
        return GphlMessages.BeamSetting(id=uuid.UUID(uuidString),
                                    wavelength=py4jBeamSetting.wavelength)


    def _GoniostatSweepSetting_to_python(self, py4jGoniostatSweepSetting):
        if py4jGoniostatSweepSetting is None:
            return None
        uuidString = py4jGoniostatSweepSetting.getId().toString()
        axisSettings = py4jGoniostatSweepSetting.getAxisSettings()
        scanAxis = py4jGoniostatSweepSetting.getScanAxis()
        return GphlMessages.GoniostatRotation(id=uuid.UUID(uuidString),
                                          scanAxis=scanAxis,
                                          **axisSettings)

    def _Sweep_to_python(self, py4jSweep):

        # NB scans are not set - where scans are present in a message,
        # the link is set from the Scan side.

        uuidString = py4jSweep.getId().toString()
        return GphlMessages.Sweep(
            goniostatSweepSetting=self._GoniostatSweepSetting_to_python(
                py4jSweep.getGoniostatSweepSetting()
            ),
            detectorSetting=self._DetectorSetting_to_python(
                py4jSweep.getDetectorSetting()
            ),
            beamSetting=self._BeamSetting_to_python(
                py4jSweep.getBeamSetting()
            ),
            start=py4jSweep.getStart(),
            width=py4jSweep.getWidth(),
            beamstopSetting=self._BeamstopSetting_to_python(
                py4jSweep.getBeamstopSetting()
            ),
            sweepGroup=py4jSweep.getSweepGroup(),
            id=uuid.UUID(uuidString)
        )

    def _ScanExposure_to_python(self, py4jScanExposure):
        uuidString = py4jScanExposure.getId().toString()
        return GphlMessages.ScanExposure(
            time=py4jScanExposure.getTime(),
            transmission=py4jScanExposure.getTransmission(),
            id=uuid.UUID(uuidString)
        )

    def _ScanWidth_to_python(self, py4jScanWidth):
        uuidString = py4jScanWidth.getId().toString()
        return GphlMessages.ScanWidth(
            imageWidth=py4jScanWidth.getImageWidth(),
            numImages=py4jScanWidth.getNumImages(),
            id=uuid.UUID(uuidString)
        )

    def _Scan_to_python(self, py4jScan, sweep):
        uuidString = py4jScan.getId().toString()
        return GphlMessages.Scan(
            width=self._ScanWidth_to_python(py4jScan.getWidth()),
            exposure=self._ScanExposure_to_python(py4jScan.getExposure()),
            imageStartNum=py4jScan.getImageStartNum(),
            start=py4jScan.getStart(),
            sweep=sweep,
            filenameParams=py4jScan.getFilenameParams(),
            id=uuid.UUID(uuidString)
        )


    # Conversion to Java
    def send_configuration_data(self, payload, correlation_id):
        self._send_py4j_response(
            self._ConfigurationData_to_java(payload), correlation_id
        )

    def send_sample_centred(self, payload, correlation_id):
        self._send_py4j_response(
            self._SampleCentred_to_java(payload), correlation_id
        )

    def send_collection_done(self, payload, correlation_id):
        self._send_py4j_response(
            self._CollectionDone_to_java(payload), correlation_id
        )

    def send_selected_lattice(self, payload, correlation_id):
        self._send_py4j_response(
            self._SelectedLattice_to_java(payload), correlation_id
        )

    def send_centring_done(self, payload, correlation_id):
        self._send_py4j_response(
            self._CentringDone_to_java(payload), correlation_id
        )

    def send_data_acquisition_start(self, payload, correlation_id=None):
        self._send_py4j_response(
            self._PriorInformation_to_java(payload), correlation_id
        )

    def _send_py4j_response(self, py4j_payload, correlation_id):
        """Create py4j message from py4j wrapper and current ids"""

        async_result = self._async_result
        if async_result is None:
            self.abort_workflow("Reply (%s) to server out of context."
                                % py4j_payload.getClass().getSimpleName())

        try:
            if self._enactment_id is None:
                enactment_id = None
            else:
                enactment_id = self._gateway.jvm.java.util.UUID.fromString(
                    self.enactment_id
                )

            if correlation_id is not None:
                correlation_id = self._gateway.jvm.java.util.UUID.fromString(
                    correlation_id
                )

            response = self._gateway.jvm.co.gphl.sdcp.py4j.Py4jMessage(
                enactment_id, correlation_id, py4j_payload
            )
        except:
            self.abort_workflow(message="Error sending reply (%s) to server"
                                % py4j_payload.getClass().getSimpleName())
        else:
            #
            self._async_result = None
            async_result.set(response)

    def _CentringDone_to_java(self, centringDone):
        return self._gateway.jvm.astra.messagebus.messages.information.CentringDoneImpl(
            self._gateway.jvm.co.gphl.beamline.v2_unstable.instrumentation.CentringStatus.valueOf(
                centringDone.status
            ),
            centringDone.timestamp,
            self._GoniostatTranslation_to_java(
                centringDone.goniostatTranslation
            )
        )

    def _ConfigurationData_to_java(self, configurationData):
        return self._gateway.jvm.astra.messagebus.messages.information.ConfigurationDataImpl(
            self._gateway.jvm.java.io.File(configurationData.location)
        )

    def _PriorInformation_to_java(self, priorInformation):

        builder = self._gateway.jvm.astra.messagebus.messages.information.PriorInformationImpl.Builder(
            self._gateway.jvm.java.util.UUID.fromString(
                str(priorInformation.sampleId)
            )
        )
        builder = builder.sampleName(priorInformation.sampleName)
        if priorInformation.referenceFile:
            builder = builder.referenceFile(self._gateway.jvm.java.net.URL(
                priorInformation.referenceFile)
            )
        builder = builder.rootDirectory(priorInformation.rootDirectory)
        # images not implemented yet - awaiting uses
        # indexingResults not implemented yet - awaiting uses
        builder = builder.userProvidedInfo(
            self._UserProvidedInfo_to_java(priorInformation.userProvidedInfo)
        )
        #
        return builder.build()

    def _SampleCentred_to_java(self, sampleCentred):

        cls = self._gateway.jvm.astra.messagebus.messages.information.SampleCentredImpl

        if sampleCentred.interleaveOrder:
            result = cls(int2Float(sampleCentred.imageWidth),
                         sampleCentred.wedgeWidth,
                         int2Float(sampleCentred.exposure),
                         int2Float(sampleCentred.transmission),
                         list(sampleCentred.interleaveOrder)
                         # self._gateway.jvm.String(sampleCentred.interleaveOrder).toCharArray()
                         )
        else:
            result = cls(int2Float(sampleCentred.imageWidth),
                         int2Float(sampleCentred.exposure),
                         int2Float(sampleCentred.transmission)
                         )

        beamstopSetting = sampleCentred.beamstopSetting
        if beamstopSetting is not None:
            result.setBeamstopSetting(
                self._BeamstopSetting_to_java(beamstopSetting)
            )

        translationSettings = sampleCentred.goniostatTranslations
        if translationSettings:
            result.setGoniostatTranslations(
                list(self._GoniostatTranslation_to_java(x)
                     for x in translationSettings)
            )
        #
        return result

    def _CollectionDone_to_java(self, collectionDone):
        proposalId = self._gateway.jvm.java.util.UUID.fromString(
            str(collectionDone.proposalId)
        )
        return self._gateway.jvm.astra.messagebus.messages.information.CollectionDoneImpl(
            proposalId, collectionDone.imageRoot, collectionDone.status
        )

    def _SelectedLattice_to_java(self, selectedLattice):
        javaFormat = self._gateway.jvm.co.gphl.beamline.v2_unstable.domain_types.IndexingFormat.valueOf(
            selectedLattice.format
        )
        return self._gateway.jvm.astra.messagebus.messages.information.SelectedLatticeImpl(
            javaFormat, selectedLattice.solution
        )

    def _BeamlineAbort_to_java(self, beamlineAbort):
        return self._gateway.jvm.astra.messagebus.messages.instructions.BeamlineAbortImpl()


    def _UserProvidedInfo_to_java(self, userProvidedInfo):

        if userProvidedInfo is None:
            return None

        builder = self._gateway.jvm.astra.messagebus.messages.information.UserProvidedInfoImpl.Builder()

        for scatterer in userProvidedInfo.scatterers:
            builder = builder.addScatterer(
                self._AnomalousScatterer_to_java(scatterer)
            )
        if userProvidedInfo.lattice:
            builder = builder.lattice(
                self._gateway.jvm.co.gphl.beamline.v2_unstable.domain_types.CrystalSystem.valueOf(
                    userProvidedInfo.lattice
                )
            )
        builder = builder.spaceGroup(userProvidedInfo.spaceGroup)
        builder = builder.cell(
            self._UnitCell_to_java(userProvidedInfo.cell)
        )
        if userProvidedInfo.expectedResolution:
            builder = builder.expectedResolution(
                int2Float(userProvidedInfo.expectedResolution)
            )
        builder = builder.anisotropic(userProvidedInfo.isAnisotropic)
        for phasingWavelength in userProvidedInfo.phasingWavelengths:
            builder.addPhasingWavelength(
                self._PhasingWavelength_to_java(phasingWavelength)
            )
        #
        return builder.build()

    def _AnomalousScatterer_to_java(self, anomalousScatterer):

        if anomalousScatterer is None:
            return None

        jvm_beamline = self._gateway.jvm.co.gphl.beamline.v2_unstable

        py4jElement = jvm_beamline.domain_types.ChemicalElement.valueOf(
            anomalousScatterer.element
        )
        py4jEdge = jvm_beamline.domain_types.AbsorptionEdge.valueOf(
            anomalousScatterer.edge
        )
        return self._gateway.jvm.astra.messagebus.messages.domain_types.AnomalousScattererImpl(
            py4jElement, py4jEdge
        )

    def _UnitCell_to_java(self, unitCell):

        if unitCell is None:
            return None

        lengths = [int2Float(x) for x in unitCell.lengths]
        angles = [int2Float(x) for x in unitCell.angles]
        return self._gateway.jvm.astra.messagebus.messages.domain_types.UnitCellImpl(
            lengths[0], lengths[1], lengths[2], angles[0], angles[1], angles[2]
        )

    def _PhasingWavelength_to_java(self, phasingWavelength):

        if phasingWavelength is None:
            return None

        javaUuid = self._gateway.jvm.java.util.UUID.fromString(
            str(phasingWavelength.id)
        )
        return self._gateway.jvm.astra.messagebus.messages.information.PhasingWavelengthImpl(
            javaUuid, int2Float(phasingWavelength.wavelength),
            phasingWavelength.role
        )

    def _GoniostatTranslation_to_java(self, goniostatTranslation):

        if goniostatTranslation is None:
            return None

        gts = goniostatTranslation
        javaUuid = self._gateway.jvm.java.util.UUID.fromString(str(gts.id))
        javaRotationId = self._gateway.jvm.java.util.UUID.fromString(
            str(gts.goniostatRotation.id)
        )
        axisSettings = dict(((x,int2Float(y))
                             for x,y in gts.axisSettings.items()))
        newRotationId = gts.newRotationId
        if newRotationId:
            javaNewRotationId = self._gateway.jvm.java.util.UUID.fromString(
                str(newRotationId)
            )
            return self._gateway.jvm.astra.messagebus.messages.instrumentation.GoniostatTranslationImpl(
                axisSettings, javaUuid, javaRotationId, javaNewRotationId
            )
        else:
            return self._gateway.jvm.astra.messagebus.messages.instrumentation.GoniostatTranslationImpl(
                axisSettings, javaUuid, javaRotationId
            )

    def _BeamstopSetting_to_java(self, beamStopSetting):

        if beamStopSetting is None:
            return None

        javaUuid = self._gateway.jvm.java.util.UUID.fromString(
            str(beamStopSetting.id)
        )
        axisSettings = dict(((x,int2Float(y))
                             for x,y in beamStopSetting.axisSettings.items()))
        return self._gateway.jvm.astra.messagebus.messages.instrumentation.BeamstopSettingImpl(
            axisSettings, javaUuid
        )

    class Java(object):
        implements = ["co.gphl.py4j.PythonListener"]
