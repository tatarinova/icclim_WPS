from pywps.Process import WPSProcess

import icclim


def defaultCallback(message,percentage):
    print ("[%s] %d" % (message,percentage))

def defaultCallback2(message,percentage):
    print ("[%s] %0.2f" % (message,percentage))

map_indice_perc = {
                    'TG10p': 10,
                    'TX10p': 10, 
                    'TN10p': 10,
                    'TG90p': 90,
                    'TX90p': 90,
                    'TN90p': 90,
                    'WSDI': 90,
                    'CSDI': 10,
                    'R75p': 75,
                    'R75TOT': 75,
                    'R95p': 95,
                    'R95TOT': 95,
                    'R99p': 99,
                    'R99TOT': 99,
}

MaxRequestLimit_bytes = 450000000

    
class ProcessPercentileIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_percentile_indice', # only mandatary attribute = same file name
                            title = 'WPS for percentile indice',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesBasePeriodIn = self.addLiteralInput(identifier = 'filesBasePeriod',
                                               title = 'Input netCDF files list (base period)',
                                               default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
        self.timeRangeBasePeriodIn = self.addLiteralInput(identifier = 'timeRangeBasePeriod', 
                                               title = 'Time range (base period)',
                                               default = None)
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               default = 'tasmax')
        
        
        self.filesStudyPeriodIn = self.addLiteralInput(identifier = 'filesStudyPeriod',
                                               title = 'Input netCDF files list (study period)',
                                               default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc'])
        
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               default = 'TX90p')
        
        self.leapNonLeapYearsIn = self.addLiteralInput(identifier = 'leapNonLeapYears',
                                               title = 'Leap or non-leap years',
                                               default = False)
        
        self.outputFileNamePercentilesIn = self.addLiteralInput(identifier = 'outputFileNamePercentiles', 
                                               title = 'Name of output file to save the dictionary with percentile values',
                                               default = './percentiles.pkl')
                
                
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               default = 'year')

        self.timeRangeStudyPeriodIn = self.addLiteralInput(identifier = 'timeRangeStudyPeriod', 
                                               title = 'Time range (study period)',
                                               default = None)
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
                                               title = 'Name of output netCDF file',
                                               default = './out_icclim.nc')
        
        self.NLevelIn = self.addLiteralInput(identifier = 'NLevel', 
                                               title = 'Number of level (if 4D variable)',
                                               default = None)
        
        #self.callbackIn = self.addLiteralInput(identifier = 'callback', 
        #                                       title = 'Callback print',
        #                                       default = defaultCallback)

        self.fileOut = self.addComplexOutput(identifier = 'output_file',
                                             title = 'Output netCDF file',
                                             formats = [
                                                        {"mimeType":"application/netcdf"} # application/x-netcdf
                                                      ])   

    def execute(self):
        
        in_files_base_period = self.filesBasePeriodIn.getValue()
        time_range_base_period = self.timeRangeBasePeriodIn.getValue()
        var_name = self.varNameIn.getValue()
        indice_name = self.indiceNameIn.getValue()
        percentile = map_indice_perc[indice_name]
        leap_nonleap_years = self.leapNonLeapYearsIn.getValue()
        out_file = self.outputFileNamePercentilesIn.getValue()
        max_request = MaxRequestLimit_bytes # in bytes
        
        in_files_study_period = self.filesStudyPeriodIn.getValue()
        time_range_study_period = self.timeRangeStudyPeriodIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        #callback = self.callbackIn.getValue()

                
        perc_dict = icclim.get_percentile_dict(in_files=in_files_base_period,
                                               var_name=var_name,
                                               percentile=percentile,
                                               window_width=5,
                                               time_range=time_range_base_period,
                                               only_leap_years=False,
                                               verbose=False,
                                               save_to_file=out_file,
                                               transfer_limit_bytes=max_request,
                                               callback = defaultCallback2)
        
        icclim.indice_perc(in_files=in_files_study_period,
                            var=var_name,
                            indice_name=indice_name,
                            percentile_dict=perc_dict,
                            slice_mode=slice_mode,
                            time_range=time_range_study_period,
                            out_file=out_file_name,
                            N_lev=level,
                            callback=defaultCallback)
