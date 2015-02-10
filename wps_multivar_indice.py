from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback

import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *

transfer_limit_Mb = 100
    
class ProcessMultivarIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_multivar_indice', # only mandatary attribute = same file name
                            title = 'WPS for multivariable indice',
                            abstract = 'WPS for multivariable indices',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)


        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               type="String",
                                               default = 'ETR')
        self.indiceNameIn.values = ['DTR', 'ETR', 'vDTR']
        
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                               title = 'Slice mode (temporal grouping to applay for calculations)',
                                               type="String",
                                               default = 'year')
        self.sliceModeIn.values = ["year","month","ONDJFM","AMJJAS","DJF","MAM","JJA","SON"]

       
        self.filesTasmaxIn = self.addLiteralInput(identifier = 'filesTasmax',
                                               title = 'Input netCDF files list (daily max temperature)',
                                               abstract="application/netcdf",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,                                               
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc,')
        
                                                
        self.varTasmaxIn = self.addLiteralInput(identifier = 'varTasmax',
                                               title = 'Variable name to process (daily max temperature)',
                                               type="String",
                                               default = 'tasmax')
        
        
        self.filesTasminIn = self.addLiteralInput(identifier = 'filesTasmin',
                                               title = 'Input netCDF files list (daily min temperature)',
                                               abstract="application/netcdf",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,      
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc,' +
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmin_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc,')
        
                                                
        self.varTasminIn = self.addLiteralInput(identifier = 'varTasmin',
                                               title = 'Variable name to process (daily min temperature)',
                                               type="String",
                                               default = 'tasmin')
        


        self.timeRangeIn = self.addLiteralInput(identifier = 'timeRange', 
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
        
        
    def callback(self,message,percentage):
        self.status.set("%s" % str(message),str(percentage));    

    def execute(self):
        
        def callback(b):
            self.callback("Processing",b)
        
        files_tasmax = [];
        files_tasmax.extend(self.filesTasmaxIn.getValue())
        

        var_tasmax = self.varTasmaxIn.getValue()
        
        files_tasmin = [];
        files_tasmin.extend(self.filesTasminIn.getValue())
        

        var_tasmin = self.varTasminIn.getValue()
        indice_name = self.indiceNameIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        time_range = self.timeRangeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        
        if(level == "None"):
            level = None
          
        if(time_range == "None"):
            time_range = None
        else:
            startdate = dateutil.parser.parse(time_range.split("/")[0])
            stopdate  = dateutil.parser.parse(time_range.split("/")[1])
            time_range = [startdate,stopdate]
        
        
        
        home = expanduser("~")
        
        self.status.set("Preparing....", 0)
        
        pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        """ URL output path """
        fileOutURL  = os.environ['PORTAL_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        """ Internal output path"""
        fileOutPath = os.environ['PORTAL_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        """ Create output directory """
        mkdir_p(fileOutPath)
        
        self.status.set("Processing input lists: " + str(files_tasmax) + " " + str(files_tasmin), 0)
        
        
        
        icclim.indice(indice_name=indice_name,
            in_files=files_tasmax,
            var_name=var_tasmax,            
            slice_mode=slice_mode,
            time_range=time_range,
            out_file=fileOutPath+out_file_name, 
            N_lev=level,            
            transfer_limit_Mbytes=transfer_limit_Mb,
            callback=callback,
            callback_percentage_start_value=0,
            callback_percentage_total=100,
            in_files2=files_tasmin,
            var_name2=var_tasmin)
        
        
        """ Set output """
        url = fileOutURL+"/"+out_file_name;
        self.opendapURL.setValue(url);
        self.status.set("ready",100);