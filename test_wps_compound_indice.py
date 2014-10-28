import wps_compound_indice0 as ProcessToTest
from pywps.Process import WPSProcess
from datetime import datetime

p1=ProcessToTest.ProcessCompoundIndice()

p1.filesBasePeriodTemperatureIn.default =  ['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/tas_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'
                                            ]

p1.filesStudyPeriodTemperatureIn.default = p1.filesBasePeriodTemperatureIn.default


p1.filesBasePeriodPrecipitationIn.default =['http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20260101-20501231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20510101-20751231.nc',
                                            'http://opendap.nmdc.eu/knmi/thredds/dodsC/IS-ENES/TESTSETS/pr_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc'
                                            ]

p1.filesStudyPeriodPrecipitationIn.default = p1.filesBasePeriodPrecipitationIn.default

p1.timeRangeBasePeriodIn.default = [datetime(2020,01,01), datetime(2025,12,31)]
p1.timeRangeStudyPeriodIn.default = [datetime(2060,01,01), datetime(2062,12,31)]

p1.outputFileNameIn.default = './compound_indice.nc'

from time import time
start = time()

p1.execute()

stop = time()
time1 = stop - start
print "time test wps percentile indice: ", time1