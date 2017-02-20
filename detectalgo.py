__author__ = 'lenord'
import pandas as pd
import numpy as np
import math
import random
import chartanalysis as chanalysis
import chartdata as chdata

#BASIC MATHEMATICAL FUNCTIONS
def moving_median(base_list,window_range):
    list_range = base_list.shape[0]
    mmedian = np.zeros(list_range)
    for window in range(0,list_range):
        window_list = base_list[window-window_range:window]
        if (np.shape(window_list)[0] == 0):
            mmedian[window] = 0
        else:
            mmedian[window] = np.median(window_list)
    return mmedian
def moving_min(base_list,window_range):
    list_range = base_list.shape[0]
    mminimum = np.zeros(list_range)
    for window in range(0,list_range):
        window_list = base_list[window-window_range:window]
        if(np.shape(window_list)[0] == 0):
            mminimum[window] = 0
        else:
            mminimum[window] = np.min(window_list)
    return mminimum
def diffSeries(data_list1,data_list2):
    return data_list1 - data_list2
def removeBelowValue(data_list,value):
    return np.array([element if element>value else 0 for element in data_list])
def timeShift(data_list,roll_size):
    shift_list = np.roll(data_list,roll_size)
    # shift_list[:roll_size] = 0
    return shift_list
def keepLastValue(data_list):
    # for index in range(2,data_list.shape[0]):
    #     if data_list[index]==0:
    #         data_list[index] = data_list[index-1]
    return data_list

#CAIDA DETECT ALGORITHM
def algoCaidaDetect(base_list):
    '''
    DETECTION ALGORITHM :
removeBelowValue(
  diffSeries(
    timeShift(
      transformNull(
        removeBelowValue(
          diffSeries(
            movingMinimum(
              darknet.ucsd-nt.non-erratic.routing.asn.3346.uniq_src_ip,
              120
            ),
            diffSeries(
              movingMedian(
                darknet.ucsd-nt.non-erratic.routing.asn.3346.uniq_src_ip,
                20
              ),
              movingMinimum(
                darknet.ucsd-nt.non-erratic.routing.asn.3346.uniq_src_ip,
                20
              )
            )
          ),
          0
        ),
        0
      ),
      "20min"
    ),
    keepLastValue(
      darknet.ucsd-nt.non-erratic.routing.asn.3346.uniq_src_ip
    )
  ),
  0
)
    '''
    mmed_20 = abs(moving_median(base_list,20))
    mmin_20 = abs(moving_min(base_list,20))
    mmin_120 = abs(moving_min(base_list,120))
    return removeBelowValue(diffSeries(timeShift(removeBelowValue(diffSeries(mmin_120,diffSeries(mmed_20,mmin_20)),
                                                                      0),20),keepLastValue(base_list)), 0)

def bayesChangePoint(base_list):
    window_start = 1050
    window_end = 1120
    offset = 1050

    data = base_list[window_start:window_end]
    windowSize = len(data)
    meanData = np.mean(data)
    meanSquareData = np.mean(np.multiply(data,data))
    squareDiff = meanSquareData - np.square(meanData)

    sumData = 0
    cumulativeSum = []

    for timestamp in range(windowSize):
        timestamp += offset
        sumData += data[timestamp]
        cumulativeSum.append(sumData)

    changePointDetect = []
    for m in range(windowSize-1):
        pos=m+1
        mscale = 4*(pos)*((windowSize)-pos)
        Q = cumulativeSum[m]-(sumData - cumulativeSum[m])
        U = -np.square(meanData*((windowSize)-2*pos) + Q)/float(mscale) + squareDiff
        changePointDetect.append(-(windowSize/float(2)-1)*math.log(windowSize*U/2) - 0.5*math.log((pos*((windowSize)-pos))))

    maxChange, changePoint, minChange = np.max(changePointDetect), np.argmax(changePointDetect), np.min(changePointDetect)
    variation = np.abs(minChange) - np.abs(maxChange)
    return changePointDetect, variation, changePoint




