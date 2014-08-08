import wps_percentile_indice as ProcessToTest
from pywps.Process import WPSProcess
from datetime import datetime

p1=ProcessToTest.ProcessPercentileIndice()

p1.timeRangeBasePeriodIn.default = [datetime(2080,01,01), datetime(2082,12,31)]
p1.timeRangeStudyPeriodIn.default = [datetime(2060,01,01), datetime(2062,12,31)]

p1.execute()