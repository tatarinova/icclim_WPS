from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback

import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *

transfer_limit_Mb = 100


map_indice_perc =   {
                    'CD': [25, 25],
                    'CW': [25, 75],
                    'WD': [75, 25],
                    'WW': [75, 75]
                    }



class ProcessCompoundIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_compound_indice', # only mandatary attribute = same file name
                            title = 'WPS for compound indice',
                            abstract = 'WPS for compound percentile-based indices',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesBasePeriodTemperatureIn = self.addLiteralInput(identifier = 'filesBasePeriodTemperature',
                                                title = 'Input netCDF files list (base period), daily mean temperature',
                                                abstract="application/netcdf",
                                                type=type("S"),
                                                minOccurs=0,
                                                maxOccurs=1024,
                                                default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')
        
        self.filesBasePeriodPrecipitationIn = self.addLiteralInput(identifier = 'filesBasePeriodPrecipitation',
                                                title = 'Input netCDF files list (base period), daily precipitation amount',
                                                abstract="application/netcdf",
                                                type=type("S"),
                                                minOccurs=0,
                                                maxOccurs=1024,                                                
                                                default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')

                                                
        self.varNameTemperatureIn = self.addLiteralInput(identifier = 'varNameTemperature',
                                                title = 'Variable name to process (daily mean temperature)',
                                                type="String",
                                                default = 'tas')
        
        self.varNamePrecipitationIn = self.addLiteralInput(identifier = 'varNamePrecipitation',
                                                title = 'Variable name to process (daily precipitation amount)',
                                                type="String",
                                                default = 'pr')
        
        self.timeRangeBasePeriodIn = self.addLiteralInput(identifier = 'timeRangeBasePeriod', 
                                                title = 'Time range of base (reference) period, e.g. 1961-01-01/1990-12-31',
                                                type="String",
                                                default = None)
        
        
        self.filesStudyPeriodTemperatureIn = self.addLiteralInput(identifier = 'filesStudyPeriodTemperature',
                                                title = 'Input netCDF files list (study period), daily mean temperature',
                                                abstract="application/netcdf",
                                                type=type("S"),
                                                minOccurs=0,
                                                maxOccurs=1024,
                                                default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')

        self.filesStudyPeriodPrecipitationIn = self.addLiteralInput(identifier = 'filesStudyPeriodPrecipitation',
                                                title = 'Input netCDF files list (study period), daily precipitation amount',
                                                abstract="application/netcdf",
                                                type=type("S"),
                                                minOccurs=0,
                                                maxOccurs=1024,
                                                default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                           'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')
        
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                                title = 'Indice name',
                                                type="String",
                                                default = 'CW')
        self.indiceNameIn.values = ['CD', 'CW', 'WD', 'WW']
        
        self.leapNonLeapYearsIn = self.addLiteralInput(identifier = 'leapNonLeapYears',
                                                title = 'Leap or non-leap years',
                                                type="String",
                                                default = False)
        
        self.outputFileNamePercentilesTemperatureIn = self.addLiteralInput(identifier = 'outputFileNamePercentilesTemperature', 
                                                title = 'Name of output file to save the dictionary with percentile values (temperature)',
                                                type="String",
                                                default = './percentiles_temperature.pkl')

        self.outputFileNamePercentilesPrecipitationIn = self.addLiteralInput(identifier = 'outputFileNamePercentilesPrecipitation', 
                                                title = 'Name of output file to save the dictionary with percentile values (precipitation)',
                                                type="String",
                                                default = './percentiles_precipitation.pkl')
                
                
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                                title = 'Slice mode (temporal grouping to applay for calculations)',
                                                default = 'year')
        self.sliceModeIn.values = ["year","month","ONDJFM","AMJJAS","DJF","MAM","JJA","SON"]

        self.timeRangeStudyPeriodIn = self.addLiteralInput(identifier = 'timeRangeStudyPeriod', 
                                                title = 'Time range, e.g. 2010-01-01/2012-12-31',
                                                type="String",
                                                default = None)
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
                                                title = 'Name of output netCDF file',
                                                type="String",
                                                default = './out_icclim.nc')
        
        self.NLevelIn = self.addLiteralInput(identifier = 'NLevel', 
                                                title = 'Number of level (if 4D variable)',
                                                type="String",
                                                default = None)

        self.opendapURL = self.addLiteralOutput(identifier = "opendapURL",title = "opendapURL");
        
        
        
    #def callback(self,message,percentage):
    #    self.status.set("%s" % str(message),str(percentage));        
                                
                                
    def execute(self):
                    
        def callback(b):
            self.callback("Processing",b)           
                    
    
        in_files_base_period_t = []
        in_files_base_period_t.extend(self.filesBasePeriodTemperatureIn.getValue())
        
        in_files_base_period_p = []
        in_files_base_period_p.extend(self.filesBasePeriodPrecipitationIn.getValue())
        
        time_range_base_period = self.timeRangeBasePeriodIn.getValue()
        time_range_study_period = self.timeRangeStudyPeriodIn.getValue()
        
        var_name_t = self.varNameTemperatureIn.getValue()
        var_name_p = self.varNamePrecipitationIn.getValue()
        
        indice_name = self.indiceNameIn.getValue()
        
        in_files_study_period_t = []
        in_files_study_period_t.extend(self.filesStudyPeriodTemperatureIn.getValue())
        
        in_files_study_period_p = []
        in_files_study_period_p.extend(self.filesStudyPeriodPrecipitationIn.getValue())
    
        percentile_t = map_indice_perc[indice_name][0]
        percentile_p = map_indice_perc[indice_name][1]
        
        leap_nonleap_years = self.leapNonLeapYearsIn.getValue()
        out_file_t = self.outputFileNamePercentilesTemperatureIn.getValue()
        out_file_p = self.outputFileNamePercentilesPrecipitationIn.getValue()
    
        slice_mode = self.sliceModeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        
        
        
        if(level == "None"):
            level = None
            
        if (leap_nonleap_years == 'False'):
            leap_nonleap_years = False
          
        if (time_range_base_period == "None"):
            time_range_base_period = None
        else:
            startdate = dateutil.parser.parse(time_range_base_period.split("/")[0])
            stopdate  = dateutil.parser.parse(time_range_base_period.split("/")[1])
            time_range_base_period = [startdate,stopdate]
        
        
        if(time_range_study_period == "None"):
            time_range_study_period = None
        else:
            startdate = dateutil.parser.parse(time_range_study_period.split("/")[0])
            stopdate  = dateutil.parser.parse(time_range_study_period.split("/")[1])
            time_range_study_period = [startdate,stopdate]
            
        
        home = expanduser("~")
        
        self.status.set("Preparing....", 0)
        
        pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        """ URL output path """
        fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        """ Internal output path"""
        fileOutPath = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        """ Create output directory """
        mkdir_p(fileOutPath)
        
        self.callback("Processing input list: "+str(files),0)  


        
        perc_dict_tas = icclim.get_percentile_dict( in_files                    = in_files_base_period_t,
                                                    var_name                    = var_name_t,
                                                    percentile                  = percentile_t,
                                                    window_width                = 5,
                                                    time_range                  = time_range_base_period,
                                                    only_leap_years             = leap_nonleap_years,
                                                    save_to_file                = fileOutPath+out_file_t,
                                                    transfer_limit_Mbytes        = transfer_limit_Mb,
                                                    callback                    = callback,
                                                    callback_percentage_start_value = 0,
                                                    callback_percentage_total   = 25,
                                                    precipitation               = False)
        
        
        perc_dict_pr = icclim.get_percentile_dict(  in_files                    = in_files_base_period_p,
                                                    var_name                    = var_name_p,
                                                    percentile                  = percentile_p,
                                                    window_width                = 5,
                                                    time_range                  = time_range_base_period,
                                                    only_leap_years             = leap_nonleap_years,
                                                    save_to_file                = fileOutPath+out_file_p,
                                                    transfer_limit_Mbytes        = transfer_limit_Mb,
                                                    callback                    = callback,
                                                    callback_percentage_start_value = 25,
                                                    callback_percentage_total   = 25,
                                                    precipitation               = True)
    
                              
        icclim.indice(indice_name=indice_name,
                      
                    in_files=in_files_study_period_t,
                    var_name=var_name_t,
                    percentile_dict=perc_dict_tas,
                    
                    slice_mode=slice_mode,
                    time_range=time_range_study_period,
                    out_file=fileOutPath+out_file_name,
                    N_lev=level,
                    transfer_limit_Mbytes=transfer_limit_Mb,
                    callback=callback,
                    callback_percentage_start_value=50,
                    callback_percentage_total=50,
                    
                    in_files2=in_files_study_period_p,
                    var_name2=var_name_p,
                    percentile_dict2=perc_dict_pr)
                    
        """ Set output """
        url = fileOutURL+"/"+out_file_name;
        self.opendapURL.setValue(url);
        self.status.set("ready",100); 