import wps_simple_indice as ProcessToTest
from pywps.Process import WPSProcess
from datetime import datetime


p1=ProcessToTest.ProcessSimpleIndice()

p1.timeRangeIn.default = [datetime(2080,01,01), datetime(2085,12,31)]

p1.execute()