from ESRF.ESRFMultiCollect import *
from detectors.LimaPilatus import Pilatus
import shutil
import logging
import os
import gevent
import time

class ID28MultiCollect(ESRFMultiCollect):
    def __init__(self, name):
        ESRFMultiCollect.__init__(self, name, PixelDetector(Pilatus), TunableEnergy())


    """
    all energy/wavelength methods to be removed when we know how to read/move energy
    """
    @task
    def set_wavelength(self, wavelength):
        return

    @task
    def set_energy(self, energy):
        return

    def getCurrentEnergy(self):
        return 12.8

    def get_wavelength(self):
        return 1.1


    def set_transmission(self, transmission_percent):
        pass

    @task
    def set_resolution(self, new_resolution):
        self.bl_control.resolution.move(new_resolution)
        while self.bl_control.resolution.motorIsMoving():
          gevent.sleep(0.1)


    @task
    def move_detector(self, detector_distance):
        self.bl_control.detector_distance.move(detector_distance)
        while self.bl_control.detector_distance.motorIsMoving():
            gevent.sleep(0.1)

    def get_detector_distance(self):
        return self.bl_control.detector_distance.getPosition()

    @task
    def set_energy(self, energy):
        return

    def open_fast_shutter(self):
        try:
            self.bl_control.diffractometer.controller.fshut.open()
        except AttributeError:
            print "No fast shutter, ignoring..."

    def close_fast_shutter(self):
        print self.bl_control.diffractometer.controller.phi.position()
        tt = time.time()
        print "waiting ready--------------->", tt
        self._detector._detector.wait_ready()
        print "waiting ready--------------->", time.time() - tt
        print self.bl_control.diffractometer.controller.phi.position()
        try:
           self.bl_control.diffractometer.controller.fshut.close()
        except AttributeError:
            print "No fast shutter, ignoring..."

    def set_transmission(self, transmission):
    	pass

    def get_transmission(self):
        return

    @task
    def prepare_intensity_monitors(self):
        pass

    def write_input_files(self, datacollection_id):
        pass

    def prepare_input_files(self, files_directory, prefix, run_number, process_directory):
        pass

    def get_archive_directory(self, directory):
        pass

    @task
    def take_crystal_snapshots(self, number_of_snapshots):
        pass

    def set_helical(self, helical_on):
        pass

    def set_helical_pos(self, helical_pos):
        pass

    def get_cryo_temperature(self):
        return 0

    @task
    def data_collection_hook(self, data_collect_parameters):
        oscillation_parameters = data_collect_parameters["oscillation_sequence"][0]
        """
        file_info = data_collect_parameters["fileinfo"]
        diagfile = os.path.join(file_info["directory"], file_info["prefix"])+"_%d_diag.dat" % file_info["run_number"]
        self.bl_control.diffractometer.controller.set_diagfile(diagfile)
        """
        self._detector.shutterless = data_collect_parameters["shutterless"]


    @task
    def do_prepare_oscillation(self, *args, **kwargs):
        """ What to do before each oscillation """
        return

    @task
    def oscil(self, start, end, exptime, npass):
        final_position = self.getObjectByRole("diffractometer").oscil(start, end, exptime)
        print "------------>final_position", final_position
        print self.bl_control.diffractometer.controller.phi.position()
        self.bl_control.diffractometer.controller.phi.move(final_position, wait=False)
        self._detector._detector.start_acquisition()

    def stop_oscillation(self):
        self.bl_control.diffractometer.controller.phi.stop()
        #self.bl_control.diffractometer.controller.musst.putget("#ABORT")

    def get_flux(self):
        return None

    def get_beam_centre(self):
        #return self.bl_control.resolution.get_beam_centre()
        return 8., 9.

    def start_acquisition(self, exptime, npass=1, first_frame=1):
        pass

    @task
    def no_oscillation(self, exptime):
        self._detector._detector.start_acquisition()
        self.open_fast_shutter()
        time.sleep(exptime)
        self.close_fast_shutter()

