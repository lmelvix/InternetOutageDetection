__author__ = 'lenord'

import numpy as np
import math
import pandas as pd
import matplotlib.pyplot as plt

def moving_median(base_list,window_range):
    list_range = base_list.shape[0]
    mmedian = np.zeros(list_range)
    for window in range(0,list_range):
        window_list = base_list[window-window_range:window]
        if (np.shape(window_list)[0] == 0):
            mmedian[window] = 0
        else:
            mmedian[window] = np.max(window_list) - np.min(window_list)
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
def suppress_min(base_list):
    list_range = base_list.shape[0]
    suppress = np.zeros(list_range)
    for window in range(0,list_range):
        if(base_list[window] >= 0.95*np.max(base_list)):
            suppress[window] = base_list[window]
        else:
            suppress[window] = 0
    return suppress
def bayesChangePoint(data,offset):
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
        if(U>0):
            changePointDetect.append(-(windowSize/float(2)-1)*math.log(windowSize*abs(U)/2) - 0.5*math.log((pos*((windowSize)-pos))))
        else :
            changePointDetect.append(np.zeros(1))

    maxChange, changePoint, minChange = np.max(changePointDetect), np.argmax(changePointDetect), np.min(changePointDetect)
    variation = np.abs(minChange) - np.abs(maxChange)
    return changePointDetect, variation, changePoint
def bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1):
    window_start = outage_start - window_buffer
    window_end = outage_end + window_buffer
    true_positive = 0.
    false_positive = 0.
    true_negative = 0.
    false_negative = 0.
    total = 0.
    print 'outage start :' + str(outage_start)
    print 'outage end : '+ str(outage_end)
    print 'window buffer : ' + str(window_buffer)
    for asn in asn_data['ASN']:
        total += 1
        if str(asn) not in asn_list:
            data = database[str(asn)][window_start:window_end]
            d_series = pd.Series(data)
            if (np.mean(data) > 30):
                step_like = bayesChangePoint(data,window_start)
                step_series = pd.Series(step_like[0])
                mean = np.mean(step_series)
                variance = np.var(step_series)
                suppress_series = suppress_min(moving_min(moving_median(step_series,5),5))
                changepoint = np.argmax(suppress_series)

                # BAYES 1.1.3 #
                if (step_like[2] > outage_start-window_buffer-window_start) and \
                        (step_like[2] < outage_end+window_buffer-window_start) and \
                        (changepoint>outage_start-(int)(window_buffer/2)-window_start) and\
                        (changepoint<outage_start+(int)(window_buffer/2)-window_start) and\
                        (mean<-1600) and (variance<1200) :

                # BAYES 1.1.2 #
                # if (step_like[1] > 70) and (step_like[2] > outage_start-window_buffer-window_start) and \
                #         (step_like[2] < outage_end+window_buffer-window_start) and \
                #         (changepoint>outage_start-(int)(window_buffer/2)-window_start) and\
                #         (changepoint<outage_start+(int)(window_buffer/2)-window_start) and\
                #         (mean<-1600) and (variance<1200) :

                # BAYES 1.1.1#
                # if (step_like[1] > 70) and (step_like[2] > outage_start-window_buffer-window_start) and \
                #         (step_like[2] < outage_end+window_buffer-window_start):

                # BAYES 1.1.0#
                # if (step_like[1] > 70) and (step_like[2] > outage_start-window_buffer-window_start) and \
                #         (step_like[2] < outage_end+window_buffer-window_start) and \
                #         (changepoint>outage_start-(int)(window_buffer/2)-window_start) and\
                #         (changepoint<outage_start+(int)(window_buffer/2)-window_start):

                    if str(asn) in asn_list1:
                        true_positive += 1
                        print "True Positive : " + str(asn) + "\t\t" + str(np.max(step_series))+ "\t\t" + str(mean) + "\t\t" + str(variance)
                    else:
                        false_positive += 1
                        print "False Positive : " + str(asn) + "\t\t" + str(np.max(step_series))+ "\t\t" + str(mean) + "\t\t" + str(variance)
                    f,axarr = plt.subplots(4)
                    axarr[0].plot(data)
                    # axarr[0].plot(d_series)
                    axarr[1].plot(step_series)
                    axarr[2].plot(suppress_series)
                    axarr[3].plot(database[str(asn)])
                    plt.show()
                else:
                    if str(asn) in asn_list1:
                        false_negative += 1
                        print "False Negative : " + str(asn)+ "\t\t" + str(np.max(step_series)) + "\t\t" + str(mean) + "\t\t" + str(variance)
                        f,axarr = plt.subplots(4)
                        axarr[0].plot(data)
                        # axarr[0].plot(d_series)
                        axarr[1].plot(step_series)
                        axarr[2].plot(suppress_series)
                        axarr[3].plot(database[str(asn)])
                        plt.show()
                    else:
                        true_negative += 1
                        # print "True Negative : " + str(asn)


            else:
                if str(asn) in asn_list1:
                    false_negative += 1
                    # print "False Negative : " + str(asn)
                    # f,axarr = plt.subplots(1)
                    # axarr[0].plot(d_series)
                    # plt.show()
                else:
                    true_negative += 1
                    # print "True Negative : " + str(asn)
        else:
            true_negative += 1
            # print 'Unqualified : ' + str(asn)

    match = true_negative+true_positive
    if true_positive == 0:
        true_positive=1
        print 'FAILED'
    accuracy = float(match/total)
    precision = float(true_positive/(true_positive+false_positive))
    recall = float(true_positive/(true_positive+false_negative))
    sensitivity = true_positive/(true_positive+false_negative) #Same as Recall
    specificity = true_negative/(true_negative+false_positive)

    print 'Accuracy : ' + str(accuracy)
    print 'Precision : ' + str(precision)
    print 'Recall : ' + str(recall)
    print 'Sensitivity : ' + str(sensitivity)
    print 'Specificity : ' + str(specificity)
    print 'True Positive : ' + str(true_positive)
    print 'True Negative : ' + str(true_negative)
    print 'False Positive : ' + str(false_positive)
    print 'False Negative : ' + str(false_negative)
    print 'Total : ' + str(total)

#CNCI
database = pd.DataFrame.from_csv('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\\results\cnci\cnci_db.csv')
asn_data = pd.read_table('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\eventData\cnci\\1080-1120\\asn.txt')
outage_start = 1080
outage_end = 1120
window_buffer = 130
asn_list = []
asn_list1 = ['3352','5610','9930','11315','22047','22927','18101']
print "CNCI\n************************************"
bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1)
print "*******************************************"

#CZECH
database = pd.DataFrame.from_csv('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\\results\\czech\\czech_db.csv')
asn_data = pd.read_table('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\eventData\\czech\\980-1040\\asn.txt')
outage_start = 980
outage_end = 1040
window_buffer = 150
asn_list = []
asn_list1 = ['3909','6697','7482','7657','9304','12578','17430']
print "CZECH\n************************************"
bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1)
print "*******************************************"

#AFNOG
database = pd.DataFrame.from_csv('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\\results\\afnog\\afnog_db.csv')
asn_data = pd.read_table('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\eventData\\afnog\\720-780\\asn.txt')
outage_start = 720
outage_end = 780
window_buffer = 150
asn_list = []
asn_list1 = ['174','7922','15857']
print "AFNOG\n************************************"
bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1)
print "*******************************************"

#JUN
database = pd.DataFrame.from_csv('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\\results\\jun\\jun_db.csv')
asn_data = pd.read_table('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\eventData\\jun\\840-900\\asn.txt')
outage_start = 840
outage_end = 900
window_buffer = (outage_end - outage_start)/2
asn_list = []
asn_list1 = ['594','2634','3329','3356','3462','4713','5391','6568','6849',
             '6877','7017','8151','8374','8551','9146','9269','9534','9658',
             '10013','10796','10838','11060','11260','11351','11426','11427',
             '11492','11955','12262','12271','12297','12400','14615','15895',
             '15958','16586','17184','17421','18812','19262','20001','20231',
             '20875','20960','23693','24139','26827','29314','33363','33588',
             '33771','34779','35002','35567','35612','45143']
print "JUN\n************************************"
bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1)
print "*******************************************"

#RD
database = pd.DataFrame.from_csv('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\\results\\rd\\rd_db.csv')
asn_data = pd.read_table('C:\Users\lenord\Documents\UCSD\CAIDA\Source_Code\Outage_Detect_2\caida-shared\eventData\\rd\\510-570\\asn.txt')
outage_start = 510
outage_end = 570
window_buffer = 10
asn_list = ['10396']
asn_list1 = ['2634','2828','2856','3243','4589','5607','5778','6983','7018','7029','9050','9498','11456','11530',
             '11976','11979','11992','12353','12357','13333','15502','15525','15732','15895','16265','17379','18881',
             '19956','20960','28347','28708','29614','31334','42298','42863']
print "RD\n************************************"
bayes_mm_detect(database,asn_data,outage_start,outage_end,window_buffer,asn_list,asn_list1)
print "*******************************************"

