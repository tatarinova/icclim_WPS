from pywps.Process import WPSProcess

import icclim

map_indice_perc =   {
                    'CD': [25, 25],
                    'CW': [25, 75],
                    'WD': [75, 25],
                    'WW': [75, 75]
                    }


MaxRequestLimit_bytes = 450000000


class ProcessCompoundIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_compound_indice', # only mandatary attribute = same file name
                            title = 'WPS for compound indice',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesBasePeriodTemperatureIn = self.addLiteralInput(identifier = 'filesBasePeriodTemperature',
                                                title = 'Input netCDF files list (base period), daily mean temperature',
                                                default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])
        
        self.filesBasePeriodPrecipitationIn = self.addLiteralInput(identifier = 'filesBasePeriodPrecipitation',
                                                title = 'Input netCDF files list (base period), daily precipitation amount',
                                                default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'])

                                                
        self.varNameTemperatureIn = self.addLiteralInput(identifier = 'varNameTemperature',
                                                title = 'Variable name to process (daily mean temperature)',
                                                default = 'tas')
        
        self.varNamePrecipitationIn = self.addLiteralInput(identifier = 'varNamePrecipitation',
                                                title = 'Variable name to process (daily precipitation amount)',
                                                default = 'pr')
        
        self.timeRangeBasePeriodIn = self.addLiteralInput(identifier = 'timeRangeBasePeriod', 
                                                title = 'Time range (base period)',
                                                default = None)
        
        
        self.filesStudyPeriodTemperatureIn = self.addLiteralInput(identifier = 'filesStudyPeriodTemperature',
                                                title = 'Input netCDF files list (study period), daily mean temperature',
                                                default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc'])

        self.filesStudyPeriodPrecipitationIn = self.addLiteralInput(identifier = 'filesStudyPeriodPrecipitation',
                                                title = 'Input netCDF files list (study period), daily precipitation amount',
                                                default = ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc'])
        
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                                title = 'Indice name',
                                                default = 'CD')
        
        self.leapNonLeapYearsIn = self.addLiteralInput(identifier = 'leapNonLeapYears',
                                                title = 'Leap or non-leap years',
                                                default = False)
        
        self.outputFileNamePercentilesTemperatureIn = self.addLiteralInput(identifier = 'outputFileNamePercentilesTemperature', 
                                                title = 'Name of output file to save the dictionary with percentile values (temperature)',
                                                default = './percentiles_temperature.pkl')

        self.outputFileNamePercentilesPrecipitationIn = self.addLiteralInput(identifier = 'outputFileNamePercentilesPrecipitation', 
                                                title = 'Name of output file to save the dictionary with percentile values (precipitation)',
                                                default = './percentiles_precipitation.pkl')
                
                
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
    
        in_files_base_period_t = self.filesBasePeriodTemperatureIn.getValue()
        in_files_base_period_p = self.filesBasePeriodPrecipitationIn.getValue()
        
        time_range_base_period = self.timeRangeBasePeriodIn.getValue()
        time_range_study_period = self.timeRangeStudyPeriodIn.getValue()
        
        var_name_t = self.varNameTemperatureIn.getValue()
        var_name_p = self.varNamePrecipitationIn.getValue()
        
        indice_name = self.indiceNameIn.getValue()
        
        in_files_study_period_t = self.filesStudyPeriodTemperatureIn.getValue()
        in_files_study_period_p = self.filesStudyPeriodPrecipitationIn.getValue()
    
        percentile_t = map_indice_perc[indice_name][0]
        percentile_p = map_indice_perc[indice_name][1]
        
        leap_nonleap_years = self.leapNonLeapYearsIn.getValue()
        out_file_t = self.outputFileNamePercentilesTemperatureIn.getValue()
        out_file_p = self.outputFileNamePercentilesPrecipitationIn.getValue()
        
        max_request = MaxRequestLimit_bytes # in bytes
    
        slice_mode = self.sliceModeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
    
        
        
        perc_dict_tas = icclim.get_percentile_dict( in_files                    = in_files_base_period_t,
                                                    var_name                    = var_name_t,
                                                    percentile                  = percentile_t,
                                                    window_width                = 5,
                                                    time_range                  = time_range_base_period,
                                                    only_leap_years             = False,
                                                    save_to_file                = out_file_t,
                                                    transfer_limit_bytes        = max_request,
                                                    callback                    = icclim.callback.defaultCallback2,
                                                    callback_percentage_total   = 25,
                                                    precipitation               = False)
        
        
        perc_dict_pr = icclim.get_percentile_dict(  in_files                    = in_files_base_period_p,
                                                    var_name                    = var_name_p,
                                                    percentile                  = percentile_p,
                                                    window_width                = 5,
                                                    time_range                  = time_range_base_period,
                                                    only_leap_years             = False,
                                                    save_to_file                = out_file_p,
                                                    transfer_limit_bytes        = max_request,
                                                    callback                    = icclim.callback.defaultCallback2,
                                                    callback_percentage_total   = 25,
                                                    precipitation               = True)
    
            
    
        icclim.indice_compound( in_files_t                                      = in_files_study_period_t,
                                var_t                                           = var_name_t,
                                percentile_dict_t                               = perc_dict_tas,
                                
                                in_files_p                                      = in_files_study_period_p,
                                var_p                                           = var_name_p,
                                percentile_dict_p                               = perc_dict_pr,
                                
                                indice_name                                     = indice_name,
    
                                slice_mode                                      = slice_mode,
                                time_range                                      = time_range_study_period,
                                out_file                                        = out_file_name,
                                N_lev                                           = level,
                                callback                                        = icclim.callback.defaultCallback2,
                                callback_percentage_total                       = 50)
