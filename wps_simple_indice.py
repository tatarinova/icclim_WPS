from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback

import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *

transfer_limit_Mb = 100

    
class ProcessSimpleIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'wps_simple_indice', # only mandatary attribute = same file name
                            title = 'WPS for simple indice',
                            abstract = 'WPS for simple indices',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)
       
        self.filesIn = self.addLiteralInput(identifier = 'files',
                                               title = 'Input netCDF files list',
                                               abstract="application/netcdf",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc,'+
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc,'+
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc,'+
                                                          'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc')
        
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'varName',
                                               title = 'Variable name to process',
                                               type="String",
                                               default = 'tasmax')
        
        self.indiceNameIn = self.addLiteralInput(identifier = 'indiceName',
                                               title = 'Indice name',
                                               type="String",
                                               default = 'SU')
        
        self.indiceNameIn.values = ["TG","TX","TN","TXx","TXn","TNx","TNn","SU","TR","CSU","GD4","FD","CFD","ID","HD17","CDD","CWD","RR","RR1","SDII","R10mm","R20mm","RX1day","RX5day","SD","SD1","SD5cm","SD50cm"]
        
        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                              title = 'Slice mode (temporal grouping to apply for calculations)',
                                              type="String",
                                              default = 'year')
        self.sliceModeIn.values = ["year","month","ONDJFM","AMJJAS","DJF","MAM","JJA","SON"]

        self.timeRangeIn = self.addLiteralInput(identifier = 'timeRange', 
                                               title = 'Time range, e.g. 2010-01-01/2012-12-31',
                                               type="String",
                                               default = None)
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'outputFileName', 
                                               title = 'Name of output netCDF file',
                                               type="String",
                                               default = 'out_icclim.nc')
        
        self.thresholdIn = self.addLiteralInput(identifier = 'threshold', 
                                               title = 'Threshold(s) for certain indices. Can be a comma separated list, e.g. 20,21,22',
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,
                                               default = None)
        
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
         
        files = [];
        files.extend(self.filesIn.getValue())
        var = self.varNameIn.getValue()
        indice_name = self.indiceNameIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        time_range = self.timeRangeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        thresholdlist = self.thresholdIn.getValue()
        thresh = None
        
        if(level == "None"):
            level = None
          
        if(time_range == "None"):
            time_range = None
        else:
            startdate = dateutil.parser.parse(time_range.split("/")[0])
            stopdate  = dateutil.parser.parse(time_range.split("/")[1])
            time_range = [startdate,stopdate]
          
        if(thresholdlist != "None"):
            if(thresholdlist[0]!="None"):
                thresh = []
                for threshold in thresholdlist:
                    thresh.append(float(threshold))
        
      
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
        
        
        icclim.indice(indice_name=indice_name,
                        in_files=files,
                        var_name=var,
                        slice_mode=slice_mode,
                        time_range=time_range,
                        out_file=fileOutPath+out_file_name,
                        threshold=thresh,
                        N_lev=level,
                        transfer_limit_Mbytes=transfer_limit_Mb,
                        callback=callback,
                        callback_percentage_start_value=0,
                        callback_percentage_total=100,
                        percentile_dict=None,
                        in_files2=None,
                        var_name2=None,
                        percentile_dict2=None)

        
        """ Set output """
        url = fileOutURL+"/"+out_file_name;
        self.opendapURL.setValue(url);
        self.status.set("ready",100);
        
        