#########################################################################################
# Script based on:                                                                      #
#                                                                                       #
# https://gis.stackexchange.com/questions/59339/generate-random-world-point-geometries  #
# https://www.shanelynn.ie/batch-geocoding-in-python-with-google-geocoding-api/         #
#                                                                                       #
#########################################################################################
from random import uniform

def newpoint():
   return uniform(-51,-45), uniform(-19, -15)

points = (newpoint() for x in range(10))
for point in points:
   print(point)

