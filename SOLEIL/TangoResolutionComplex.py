# -*- coding: utf-8 -*-
import math
import logging
#import time
#import qt

#from SimpleDevice import SimpleDevice
from PyTango import DeviceProxy
from HardwareRepository import BaseHardwareObjects
from HardwareRepository import HardwareRepository
#from SpecClient import SpecCommand
# Changed for PILATUS 6M
DETECTOR_DIAMETER = 315 #424.
# specState = {
#     'NOTINITIALIZED':   0,
#     'UNUSABLE':         1,
#     'READY':            2,
#     'MOVESTARTED':      3,
#     'MOVING':           4,
#     'ONLIMIT':          5}
class SpecMotor:
    (NOTINITIALIZED, UNUSABLE, READY, MOVESTARTED, MOVING, ONLIMIT) = (0,1,2,3,4,5)

class TangoResolutionComplex(BaseHardwareObjects.Equipment):
#     resoState = {
#         None: 'unknown',
#         'UNKNOWN': 'unknown',
#         'CLOSE': 'closed',
#         'OPEN': 'opened',
#         'MOVING': 'moving',
#         'FAULT': 'fault',
#         'DISABLE': 'disabled',
#         'OFF': 'fault',
#         'ON': 'unknown'
#         }
        
    stateDict = {
         "UNKNOWN": 0,
         "OFF":     1,
         "ALARM":   1,
         "STANDBY": 2,
         "RUNNING": 4,
         "MOVING":  4,
         "2":       2,
         "1":       1}
   
    
    def _init(self):
        self.currentResolution = None
        self.currentDistance = None
        self.currentWavelength = None
        self.currentEnergy = None
        self.connect("equipmentReady", self.equipmentReady)
        self.connect("equipmentNotReady", self.equipmentNotReady)
        #self.device = SimpleDevice(self.getProperty("tangoname"), waitMoves = False, verbose=False)
        self.device = DeviceProxy(self.getProperty("tangoname"))
        #self.device.timeout = 3000 # Setting timeout to 3 sec
        
        #self.monodevice = SimpleDevice(self.getProperty("tangoname2"), waitMoves = False, verbose=False)

        hobj = self.getProperty("BLEnergy")
        logging.getLogger("HWR").debug('TangoResolution: load specify the %s hardware object' % hobj)
        self.blenergyHO = None
        if hobj is not None:
            try:
                self.blenergyHO=HardwareRepository.HardwareRepository().getHardwareObject(hobj)
            except:
                logging.getLogger("HWR").error('TangoResolutionComplex: BLEnergy is not defined in resolution equipment %s', str(self.name()))
       
        if self.blenergyHO is not None:
            #self.connect(self.blenergyHO, "energyChanged",self.energyChanged)
            self.blenergyHO.connect("energyChanged",self.energyChanged)
        else:
            logging.info('TANGORESOLUTION : BLENERGY is not defined in TangoResolution equipment %s', str(self.name()))

        #self.connect(self.blenergyHO,, "energyChanged",self.energyChanged)
        #self.connect(self.beam_info_hwobj, 
        #                 "beamPosChanged", 
        #                 self.beam_position_changed)
            #self.blenergyHO.connectSignal('energyChanged', self.energyChanged)
        # creer un chanel sur l'energy: pour faire un update 
        positChan = self.getChannelObject("position") # utile seulement si statechan n'est pas defini dans le code
        positChan.connectSignal("update", self.positionChanged)
        stateChan = self.getChannelObject("state") # utile seulement si statechan n'est pas defini dans le code
        stateChan.connectSignal("update", self.stateChanged)
        
        self.currentDistance = self.device.position
        self.currentEnergy = self.blenergyHO.getCurrentEnergy()
        self.currentWavelength = self.blenergyHO.getCurrentWavelength()
        return BaseHardwareObjects.Equipment._init(self)

        
    def init(self):
        #self.detm = self.getDeviceByRole("detm")
        #self.dtox = self.getDeviceByRole("dtox")
        #self.dist2res = self.getCommandObject("dist2res")
        #self.res2dist = self.getCommandObject("res2dist")
        self.__resLimitsCallback = None
        
        
        self.__resLimitsErrCallback = None
        self.__resLimits = {}

        #self.connect(self.device, "stateChanged", self.detmStateChanged)
        #self.dist2res.connectSignal("commandReplyArrived", self.newResolution)
        #self.res2dist.connectSignal("commandReplyArrived", self.newDistance)
    
    def positionChanged(self, value):
        res = self.dist2res(value)
        try:
            logging.getLogger("HWR").debug("%s: TangoResolution.positionChanged: %.3f", self.name(), res)
        except:
            logging.getLogger("HWR").error("%s: TangoResolution not responding, %s", self.name(), '')       
        self.emit('positionChanged', (res,))

    
    def getState(self):
        return TangoResolutionComplex.stateDict[str(self.device.State())] 

                
    def equipmentReady(self):
        self.emit("deviceReady")


    def equipmentNotReady(self):    
        self.emit("deviceNotReady")
        

    def get_value(self):
        return self.getPosition()
        

    def getPosition(self):
        #if self.currentResolution is None:
        self.recalculateResolution()
        return self.currentResolution

    def energyChanged(self, energy,wave=None):
        #logging.getLogger("HWR").debug(" %s energychanged : %.3f wave is %.3f", self.name(), energy,wave)
        if self.currentEnergy is None:
            self.currentEnergy = energy
        if type(energy) is not float:
            logging.getLogger("HWR").error("%s: TangoResolution Energy not a float: %s", energy, '')
            return
        if abs(self.currentEnergy - energy) > 0.0002:
            self.currentEnergy = energy # self.blenergyHO.getCurrentEnergy()
            self.wavelengthChanged(self.blenergyHO.getCurrentWavelength())
        
    def wavelengthChanged(self, wavelength):
        self.currentWavelength = wavelength
        self.recalculateResolution()
        
    def recalculateResolution(self):
        self.currentDistance = self.device.position
        self.currentResolution = self.dist2res(self.currentDistance)
        if self.currentResolution is None:
            return
        self.newResolution(self.currentResolution) 

    def newResolution(self, res):      
        if self.currentResolution is None:
            self.currentResolution = self.recalculateResolution()
        logging.getLogger().info("new resolution = %.3f" % res)
        self.currentResolution = res
        self.emit("positionChanged", (res, ))
    
    def connectNotify(self, signal):
        #logging.getLogger("HWR").debug("%s: TangoResolution.connectNotify, : %s", \
        #                                                  self.name(), signal)
        if signal == "stateChanged":
            self.stateChanged(self.getState())
        
        #elif signal == 'limitsChanged':
        #    self.motorLimitsChanged()
            
        elif signal == 'positionChanged':
            self.positionChanged(self.device.position)
#        self.setIsReady(True)

    def stateChanged(self, state):
        #logging.getLogger("HWR").debug("%s: TangoResolution.stateChanged: %s"\
        #                                            % (self.name(), state))
        try:
            self.emit('stateChanged', (TangoResolutionComplex.stateDict[str(state)], ))
        except KeyError:
            self.emit('stateChanged', (TangoResolutionComplex.stateDict['UNKNOWN'], )) #ms 2015-03-26 trying to get rid of the fatal error with connection to detector ts motor


    def getLimits(self, callback=None, error_callback=None):
        #low, high = self.device.getLimits("position")
        # MS 18.09.2012 adapted for use without SimpleDevice
        position_info = self.device.attribute_query("position")
        low  = float(position_info.min_value)
        high = float(position_info.max_value)
        logging.getLogger("HWR").debug("%s: DetectorDistance.getLimits: [%.2f - %.2f]"\
                                                    % (self.name(), low, high))
        
        if callable(callback):
            self.__resLimitsCallback = callback
            self.__resLimitsErrCallback = error_callback

            self.__resLimits = {}
            rlow = self.dist2res(low, callback=self.__resLowLimitCallback, \
                                   error_callback=self.__resLimitsErrCallback)
            rhigh = self.dist2res(high, callback=self.__resHighLimitCallback,\
                                   error_callback=self.__resLimitsErrCallback)
        else:
            #rlow, rhigh = map(self.dist2res, self.device.getLimits("position"))
            # MS 18.09.2012 adapted for use without SimpleDevice
            #rhigh = self.device.attribute_query("positon").max_value
            rlow  = self.dist2res(low)
            rhigh   = self.dist2res(high)
            #position_info = self.device.attribute_query("position")
            #rlow  = float(position_info.min_value)
            #rhigh = float(position_info.max_value)
            
        
        logging.getLogger("HWR").debug("%s: TangoResolution.getLimits: [%.3f - %.3f]"\
                                                     % (self.name(), rlow, rhigh))
        return (rlow, rhigh)


    def isSpecConnected(self):
        #logging.getLogger().debug("%s: TangoResolution.isSpecConnected()" % self.name())
        return True
    
    def __resLowLimitCallback(self, rlow):
        self.__resLimits["low"]=float(rlow)

        if len(self.__resLimits) == 2:
            if callable(self.__resLimitsCallback):
              self.__resLimitsCallback((self.__resLimits["low"], self.__resLimits["high"]))
            self.__resLimitsCallback = None
            self.__dist2resA1 = None
            self.__dist2resA2 = None


    def __resHighLimitCallback(self, rhigh):
        self.__resLimits["high"]=float(rhigh)

        if len(self.__resLimits) == 2:
            if callable(self.__resLimitsCallback):
              self.__resLimitsCallback((self.__resLimits["low"], self.__resLimits["high"]))
            self.__resLimitsCallback = None
            self.__dist2resA1 = None
            self.__dist2resA2 = None
            

    def __resLimitsErrCallback(self):
        if callable(self.__resLimitsErrCallback):
            self.__resLimitsErrCallback()
            self.__resLimitsErrCallback = None
            self.__dist2resA1 = None
            self.__dist2resA2 = None


    def move(self, res, mindist=114, maxdist=1000):
        self.currentWavelength = self.blenergyHO.getCurrentWavelength()
        distance = self.res2dist(res)
        if distance >= mindist and distance <= maxdist:
            self.device.position = distance
        elif distance < mindist:
            logging.getLogger("user_level_log").warning("TangoResolution: requested resolution is above limit for specified energy, moving to maximum allowed resolution")
            self.device.position = mindist
        elif distance > maxdist:
            logging.getLogger("user_level_log").warning("TangoResolution: requested resolution is below limit for specified energy, moving to minimum allowed resolution")
            self.device.position = maxdist
            

    def newDistance(self, dist):
        self.device.position = dist

    def motorIsMoving(self):
        return self.device.state().name in ['MOVING', 'RUNNING']
        
    def stop(self):
        try:
            self.device.Stop()
        except:
            logging.getLogger("HWR").err("%s: TangoResolution.stop: error while trying to stop!", self.name())
            pass
        
    def dist2res(self, Distance, callback=None, error_callback=None):

        #Distance = float(Distance)# MS 2015-03-26 moving statement inside try loop
        try:
            Distance = float(Distance)
            #Wavelength = self.monodevice._SimpleDevice__DevProxy.read_attribute("lambda").value
            if self.currentWavelength is None:
                self.currentWavelength = self.blenergyHO.getCurrentWavelength()
            thetaangle2 = math.atan(DETECTOR_DIAMETER/2./Distance)
            Resolution = 0.5*self.currentWavelength /math.sin(thetaangle2/2.)
            if callable(callback):
                callback(Resolution)
            return Resolution
        except:
            if callable(error_callback):
                error_callback()
    
    def dist2resWaveLenght(self,wavelength, Distance, callback=None, error_callback=None):

        #Distance = float(Distance)# MS 2015-03-26 moving statement inside try loop
        try:
            #Distance = Distance
            #Wavelength = self.monodevice._SimpleDevice__DevProxy.read_attribute("lambda").value
            
            thetaangle2 = math.atan(DETECTOR_DIAMETER/2./Distance)
            Resolution = 0.5*wavelength /math.sin(thetaangle2/2.)
            if callable(callback):
                callback(Resolution)
            return Resolution
        except:
            if callable(error_callback):
                error_callback()

    
    def res2dist(self, Resolution):
        #print "********* In res2dist with ", Resolution
        Resolution = float(Resolution)
        #Wavelength = self.monodevice._SimpleDevice__DevProxy.read_attribute("lambda").value
        if self.currentWavelength is None:
            self.currentWavelength = self.blenergyHO.getCurrentWavelength()
        thetaangle=math.asin(self.currentWavelength / 2. / Resolution)
        Distance=DETECTOR_DIAMETER/2./math.tan(2.*thetaangle)
        #print "********* Distance ", Distance
        return Distance
    
