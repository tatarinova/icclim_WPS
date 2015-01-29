from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback
#cb = callback.defaultCallback
cb = callback.defaultCallback2

transfer_limit_Mb = 500

    
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
                                               default = ['http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               default = 'tasmax')
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               default = 'SU')
        
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               default = 'JJA')

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
      
        
        icclim.indice(indice_name=indice_name,
                        in_files=files,
                        var_name=var,
                        slice_mode=slice_mode,
                        time_range=time_range,
                        out_file=out_file_name,
                        threshold=thresh,
                        N_lev=level,
                        transfer_limit_Mbytes=transfer_limit_Mb,
                        callback=cb,
                        callback_percentage_start_value=0,
                        callback_percentage_total=100,
                        percentile_dict=None,
                        in_files2=None,
                        var_name2=None,
                        percentile_dict2=None)
        
        
        
        
        
        
        
        print 'Success!!!'
