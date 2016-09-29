"""
Capture images from Leica DMC2900 camera
"""
from HardwareRepository import BaseHardwareObjects
from bliss.controllers import _leica_usb as leica_usb
from HardwareRepository.HardwareObjects.Camera import JpegType
from Qub.CTools import pixmaptools
import gevent
import time

class LeicaDM2900Video(BaseHardwareObjects.Device):

    def __init__(self, name):
	BaseHardwareObjects.Device.__init__(self, name)
        self.__contrastExists = False
        self.__brightnessExists = False
        self.__gainExists = False
        self.__gammaExists = False
        self.scaling = pixmaptools.LUT.Scaling()
        self.scalingType = pixmaptools.LUT.Scaling.RGB24

    def init(self):
        self.camera = leica_usb.LeicaCamera()

 	self.setIsReady(True)

    def imageType(self):
        return JpegType()

    #############   CONTRAST   #################
    def contrastExists(self):
        return self.__contrastExists

    def setContrast(self, contrast):
        pass

    def getContrast(self):
	return 

    def getContrastMinMax(self):
	return 

    #############   BRIGHTNESS   #################
    def brightnessExists(self):
        return self.__brightnessExists

    def setBrightness(self, brightness):
	pass

    def getBrightness(self):
	return 

    def getBrightnessMinMax(self):
	return 

    #############   GAIN   #################
    def gainExists(self):
        return self.__gainExists

    def setGain(self, gain):
	self.video.setGain(gain)

    def getGain(self):
	return self.video.getGain()

    def getGainMinMax(self):
	return 

    #############   GAMMA   #################
    def gammaExists(self):
        return self.__gammaExists

    def setGamma(self, gamma):
 	pass

    def getGamma(self):
	return 

    def getGammaMinMax(self):
	return (0, 1)

    def setLive(self, mode):
        return
    
    def getWidth(self):
        return 2048 #1024
	
    def getHeight(self):
        return 1024 #768

    def _do_imagePolling(self, sleep_time):
        while True:
              self.newImage()
              time.sleep(sleep_time)
	     	
    def connectNotify(self, signal):
	if signal=="imageReceived":
            self.__imagePolling = gevent.spawn(self._do_imagePolling, self.getProperty("interval")/1000.0)

    def takeSnapshot(self, *args, **kw):
        filename = args[0]
        #qimage.save(filename, "PNG")
 
    def newImage(self):
        img = self.camera.get_next_image().get_binned((2,2))
        raw_buffer = img.data_aligned()
        self.scaling.autoscale_min_max(raw_buffer, img.cols, img.rows, self.scalingType)
        validFlag, qimage = pixmaptools.LUT.raw_video_2_image(raw_buffer,
                                                              img.cols, img.rows,
                                                              self.scalingType,
                                                              self.scaling)
        if validFlag:
            self.emit("imageReceived", qimage, qimage.width(), qimage.height(), True)

