from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback

import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *

transfer_limit_Mb = 100

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
                            abstract = 'WPS for percentile-based indices',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesBasePeriodIn = self.addLiteralInput(identifier = 'filesBasePeriod',
                                               title = 'Input netCDF files list (base period)',
                                               abstract="application/netcdf",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')
        
        self.timeRangeBasePeriodIn = self.addLiteralInput(identifier = 'timeRangeBasePeriod', 
                                               title = 'Time range of base (reference) period, e.g. 1961-01-01/1990-12-31',
                                               type="String",
                                               default = None)
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               type="String",
                                               default = 'tas')
        
        
        self.filesStudyPeriodIn = self.addLiteralInput(identifier = 'filesStudyPeriod',
                                               title = 'Input netCDF files list (study period)',
                                               abstract="application/netcdf",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')
        
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               type="String",
                                               default = 'TG90p')
        self.indiceNameIn.values = ['TG10p', 'TX10p', 'TN10p', 'TG90p', 'TX90p', 'TN90p', 'WSDI', 'CSDI', 'R75p', 'R75TOT', 'R95p', 'R95TOT', 'R99p', 'R99TOT']
        
        self.leapNonLeapYearsIn = self.addLiteralInput(identifier = 'leapNonLeapYears',
                                               title = 'Leap or non-leap years',
                                               type="String",
                                               default = False)
        
        self.outputFileNamePercentilesIn = self.addLiteralInput(identifier = 'outputFileNamePercentiles', 
                                               title = 'Name of output file to save the dictionary with percentile values',
                                               type="String",
                                               default = './percentiles.pkl')
                
                
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               type="String",
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
        
        indice_name = self.indiceNameIn.getValue()
        
        percentile = map_indice_perc[indice_name]
        precip_bool_value = map_indice_perc_precipitation[indice_name]
        
        in_files_base_period = []
        in_files_base_period.extend(self.filesBasePeriodIn.getValue())      
        
        time_range_base_period = self.timeRangeBasePeriodIn.getValue()
        
        var_name = self.varNameIn.getValue()
                
        leap_nonleap_years = self.leapNonLeapYearsIn.getValue()
        out_file = self.outputFileNamePercentilesIn.getValue()
        
        in_files_study_period = self.filesStudyPeriodIn.getValue()
        
        in_files_study_period = []
        in_files_study_period.extend(self.filesStudyPeriodIn.getValue())
        
        time_range_study_period = self.timeRangeStudyPeriodIn.getValue()

        slice_mode = self.sliceModeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()

        
        if (level == "None"):
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
        
        
      
        pd = icclim.get_percentile_dict(in_files=in_files_base_period,
                                        var_name=var_name,
                                        percentile=percentile,                                        
                                        window_width=5,                                        
                                        time_range=time_range_base_period,
                                        only_leap_years=leap_nonleap_years,
                                        save_to_file=fileOutPath+out_file,
                                        transfer_limit_Mbytes=transfer_limit_Mb,
                                        callback=callback,
                                        callback_percentage_start_value=0,
                                        callback_percentage_total=50,
                                        precipitation=precip_bool_value)
    
       
        
        icclim.indice(indice_name=indice_name,
                        in_files=in_files_study_period,
                        var_name=var_name,
                        slice_mode=slice_mode,
                        time_range=time_range_study_period,
                        out_file=fileOutPath+out_file_name,
                        N_lev=level,
                        transfer_limit_Mbytes=transfer_limit_Mb,
                        callback=callback,
                        callback_percentage_start_value=50,
                        callback_percentage_total=50,
                        percentile_dict=pd)
        
        
        """ Set output """
        url = fileOutURL+"/"+out_file_name;
        self.opendapURL.setValue(url);
        self.status.set("ready",100);
        
        