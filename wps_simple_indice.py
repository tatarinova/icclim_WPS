from pywps.Process import WPSProcess

import icclim


def defaultCallback(message,percentage):
    print ("[%s] %d" % (message,percentage))
    
class ProcessSimpleIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_simple_indice', # only mandatary attribute = same file name
                            title = 'WPS for simple indice',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesIn = self.addLiteralInput(identifier = 'files',
                                               title = 'Input netCDF files list',
                                               default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               default = 'tasmax')
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               default = 'SU')
        
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               default = 'year')

        self.timeRangeIn = self.addLiteralInput(identifier = 'timeRange', 
                                               title = 'Time range',
                                               default = None)
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
                                               title = 'Name of output netCDF file',
                                               default = './out_icclim.nc')
        
        self.thresholdIn = self.addLiteralInput(identifier = 'threshold', 
                                               title = 'Threshold(s) for certain indices',
                                               default = None)
        
        self.NLevelIn = self.addLiteralInput(identifier = 'NLevel', 
                                               title = 'Number of level (if 4D variable)',
                                               default = None)
        
        self.callbackIn = self.addLiteralInput(identifier = 'callback', 
                                               title = 'Callback print',
                                               default = defaultCallback)

        self.fileOut = self.addComplexOutput(identifier = 'output_file',
                                             title = 'Output netCDF file',
                                             formats = [
                                                        {"mimeType":"application/netcdf"} # application/x-netcdf
                                                      ])   

    def execute(self):
        
        files = self.filesIn.getValue()
        var = self.varNameIn.getValue()
        indice_name = self.indiceNameIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        time_range = self.timeRangeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        thresh = self.thresholdIn.getValue()
        callback = self.callbackIn.getValue()

        icclim.indice(in_files = files,
                        var = var,
                        indice_name = indice_name,
                        slice_mode = slice_mode,
                        time_range = time_range,
                        out_file = out_file_name,
                        threshold = thresh,
                        N_lev = level,
                        callback = defaultCallback)
        

        print 'Success!!!'
