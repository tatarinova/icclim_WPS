import wps_percentile_indice as ProcessToTest
from pywps.Process import WPSProcess
from datetime import datetime

p1=ProcessToTest.ProcessPercentileIndice()


indice = 'TG10p'

a = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/'
files_tas = [a + 'tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
             a + 'tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
             a + 'tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
             a + 'tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc']
files_tasmax = [a + 'tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
             a + 'tasmax_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
             a + 'tasmax_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
             a + 'tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc']
files_tasmin = [a + 'tasmin_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
             a +'tasmin_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
             a +'tasmin_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
             a +'tasmin_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc']
files_pr = [a + 'pr_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
             a + 'pr_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
             a + 'pr_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
             a + 'pr_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc']

map_indice_file = {
                    'TG': files_tas,
                    'GD4': files_tas,
                    'HD17': files_tas,
                    'TG10p': files_tas,
                    'TG90p': files_tas,
                                        
                    'TX': files_tasmax,
                    'TXx': files_tasmax,
                    'TXn': files_tasmax,
                    'SU': files_tasmax,
                    'CSU': files_tasmax,
                    'ID': files_tasmax,
                    'TX10p': files_tasmax,
                    'TX90p': files_tasmax,
                    'WSDI': files_tasmax,
                    
                    'TN': files_tasmin,
                    'TNx': files_tasmin,
                    'TNn': files_tasmin,
                    'TR': files_tasmin,
                    'FD': files_tasmin,
                    'CFD': files_tasmin,
                    'TN10p': files_tasmin,
                    'TN90p': files_tasmin,
                    'CSDI': files_tasmin,

                    
                    'DTR': (files_tasmax, files_tasmin),
                    'ETR': (files_tasmax, files_tasmin),
                    'vDTR': (files_tasmax, files_tasmin),

                    'CDD': files_pr,
                    'CWD': files_pr,
                    'RR': files_pr,
                    'RR1': files_pr,
                    'SDII': files_pr,
                    'R10mm': files_pr,
                    'R20mm': files_pr,
                    'RX1day': files_pr,
                    'RX5day': files_pr,
                    'SD': files_pr,
                    'SD1': files_pr,
                    'SD5cm': files_pr,
                    'SD50cm': files_pr,
                    
                    'R75p': files_pr,
                    'R75TOT': files_pr,
                    'R95p': files_pr,
                    'R95TOT': files_pr,                    
                    'R99p': files_pr,
                    'R99TOT': files_pr,
                    
                    'CD': (files_tas, files_pr),
                    'CW': (files_tas, files_pr),
                    'WD': (files_tas, files_pr),
                    'WW': (files_tas, files_pr),                    
}


p1.timeRangeBasePeriodIn.default = [datetime(2080,01,01), datetime(2082,12,31)]
p1.timeRangeStudyPeriodIn.default = [datetime(2060,01,01), datetime(2062,12,31)]
#p1.indiceNameIn.default = indice
#p1.filesBasePeriodIn.default = map_indice_file[indice]
#p1.filesStudyPeriodIn.default = map_indice_file[indice]
#p1.varNameIn.default = 'tas'

p1.execute()