from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback
#cb = callback.defaultCallback
cb = callback.defaultCallback2

transfer_limit_Mb = 500

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

map_indice_perc_precipitation = {
                    'TG10p': False,
                    'TX10p': False, 
                    'TN10p': False,
                    'TG90p': False,
                    'TX90p': False,
                    'TN90p': False,
                    'WSDI': False,
                    'CSDI': False,
                    'R75p': True,
                    'R75TOT': True,
                    'R95p': True,
                    'R95TOT': True,
                    'R99p': True,
                    'R99TOT': True,
}

    
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
                                               default = ['http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
        self.timeRangeBasePeriodIn = self.addLiteralInput(identifier = 'timeRangeBasePeriod', 
                                               title = 'Time range (base period)',
                                               default = None)
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               default = 'tas')
        
        
        self.filesStudyPeriodIn = self.addLiteralInput(identifier = 'filesStudyPeriod',
                                               title = 'Input netCDF files list (study period)',
                                               default = ['http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               default = 'TG90p')
        
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

        self.fileOut = self.addComplexOutput(identifier = 'output_file',
                                             title = 'Output netCDF file',
                                             formats = [
                                                        {"mimeType":"application/netcdf"} # application/x-netcdf
                                                      ])   

    def execute(self):
        
        indice_name = self.indiceNameIn.getValue()
        
        percentile = map_indice_perc[indice_name]
        precip_bool_value = map_indice_perc_precipitation[indice_name]
        
        in_files_base_period = self.filesBasePeriodIn.getValue()
        time_range_base_period = self.timeRangeBasePeriodIn.getValue()
        var_name = self.varNameIn.getValue()
                
        leap_nonleap_years = self.leapNonLeapYearsIn.getValue()
        out_file = self.outputFileNamePercentilesIn.getValue()
        
        in_files_study_period = self.filesStudyPeriodIn.getValue()
        time_range_study_period = self.timeRangeStudyPeriodIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()


      
        pd = icclim.get_percentile_dict(in_files=in_files_base_period,
                                        var_name=var_name,
                                        percentile=percentile,                                        
                                        window_width=5,                                        
                                        time_range=time_range_base_period,
                                        only_leap_years=leap_nonleap_years,
                                        save_to_file=out_file,
                                        transfer_limit_Mbytes=transfer_limit_Mb,
                                        callback=cb,
                                        callback_percentage_start_value=0,
                                        callback_percentage_total=50,
                                        precipitation=precip_bool_value)
    
       
        
        icclim.indice(indice_name=indice_name,
                        in_files=in_files_study_period,
                        var_name=var_name,
                        slice_mode=slice_mode,
                        time_range=time_range_study_period,
                        out_file=out_file_name,
                        N_lev=level,
                        transfer_limit_Mbytes=transfer_limit_Mb,
                        callback=cb,
                        callback_percentage_start_value=50,
                        callback_percentage_total=50,
                        percentile_dict=pd)
        
        