from HardwareRepository.BaseHardwareObjects import Equipment
import tempfile
import logging
import math
import os
import time
from HardwareRepository import HardwareRepository
import MiniDiff
from HardwareRepository import EnhancedPopen
import copy
import gevent
import sample_centring

#ID28GONIO = None

class ID28Gonio(MiniDiff.MiniDiff):
    def init(self):
        """
        global ID28GONIO
        ID28GONIO = self
        """
        self.controller = self.getObjectByRole("controller")
        self.timeout = 3
        self.final_position = None
        
        MiniDiff.MiniDiff.init(self)
        self.centringPhiy.direction = -1


    def oscil(self, *args, **kwargs):
        #this will be when fast shutter implemented
        #self.controller.diffractometer.oscil(*args, **kwargs)
        final_position = self.controller.diffractometer.oscil_prepare(*args, **kwargs)
        return final_position
        
 
    def getCalibrationData(self, offset):
        #return (1.0/self.x_calib.getValue(), 1.0/self.y_calib.getValue())
        return (1,1)

    def emitCentringSuccessful(self):
        return MiniDiff.MiniDiff.emitCentringSuccessful(self)


    def getPositions(self):
        pos = { "phi": float(self.phiMotor.getPosition()),
                "focus": float(self.focusMotor.getPosition()) if self.focusMotor else None,
                "phiy": float(self.phiyMotor.getPosition()) if self.phiyMotor else None,
                "phiz": float(self.phizMotor.getPosition()) if self.phizMotor else None,
                "sampx": float(self.sampleXMotor.getPosition()) if self.sampleXMotor else None,
                "sampy": float(self.sampleYMotor.getPosition()) if self.sampleYMotor else None,
                "zoom": float(self.zoomMotor.getPosition()) if self.zoomMotor else None,
                "kappa": float(self.kappaMotor.getPosition()) if self.kappaMotor else None,
                "kappa_phi": float(self.kappaPhiMotor.getPosition()) if self.kappaPhiMotor else None}
        return pos

    def moveMotors(self, roles_positions_dict):
        if not self.in_kappa_mode():
            try:
                roles_positions_dict.pop["kappa"]
                roles_positions_dict.pop["kappa_phi"]
            except:
                pass
            
        self.moveSyncMotors(roles_positions_dict, wait=True)

    def start3ClickCentring(self, sample_info=None):
         self.currentCentringProcedure.link(self.manualCentringDone)

    def moveToCentredPosition(self):
        pass

"""
def set_light_in(light, light_motor, zoom):
    ID28GONIO.getDeviceByRole("flight").move(0)
    ID28GONIO.getDeviceByRole("lightInOut").actuatorIn()

MiniDiff.set_light_in = set_light_in
"""
