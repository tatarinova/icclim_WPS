from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback
#cb = callback.defaultCallback
cb = callback.defaultCallback2

transfer_limit_Mb = 500
    
class ProcessMultivarIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_multivar_indice', # only mandatary attribute = same file name
                            title = 'WPS for multivariable indice',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesTasmaxIn = self.addLiteralInput(identifier = 'filesTasmax',
                                               title = 'Input netCDF files list (daily max temperature)',
                                               default = ['http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
                                                
        self.varTasmaxIn = self.addLiteralInput(identifier = 'varTasmax',
                                               title = 'Variable name to process (daily max temperature)',
                                               default = 'tasmax')
        
        
        self.filesTasminIn = self.addLiteralInput(identifier = 'filesTasmin',
                                               title = 'Input netCDF files list (daily min temperature)',
                                               default = ['http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
                                                
        self.varTasminIn = self.addLiteralInput(identifier = 'varTasmin',
                                               title = 'Variable name to process (daily min temperature)',
                                               default = 'tasmin')
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               default = 'ETR')
        
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               default = 'year')

        self.timeRangeIn = self.addLiteralInput(identifier = 'timeRange', 
                                               title = 'Time range',
                                               default = None)
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
                                               title = 'Name of output netCDF file',
                                               default = './out_icclim.nc')
        
        self.NLevelIn = self.addLiteralInput(identifier = 'NLevel', 
                                               title = 'Number of level (if 4D variable)',
                                               default = None)

        self.fileOut = self.addComplexOutput(identifier = 'output_file',
                                             title = 'Output netCDF file',
                                             formats = [
                                                        {"mimeType":"application/netcdf"} # application/x-netcdf
                                                      ])   

    def execute(self):
        
        files_tasmax = self.filesTasmaxIn.getValue()
        var_tasmax = self.varTasmaxIn.getValue()
        files_tasmin = self.filesTasminIn.getValue()
        var_tasmin = self.varTasminIn.getValue()
        indice_name = self.indiceNameIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        time_range = self.timeRangeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()

       
        
        icclim.indice(indice_name=indice_name,
            in_files=files_tasmax,
            var_name=var_tasmax,            
            slice_mode=slice_mode,
            time_range=time_range,
            out_file=out_file_name, 
            N_lev=level,            
            transfer_limit_Mbytes=transfer_limit_Mb,
            callback=cb,
            callback_percentage_start_value=0,
            callback_percentage_total=100,
            in_files2=files_tasmin,
            var_name2=var_tasmin)
        
        
        print 'Success!!!'