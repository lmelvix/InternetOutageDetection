import pandas as pd
import numpy as np
import csv
import chartfetch as chfetch

class outageDetect(object):
    @staticmethod
    def detectAnyOutage(rawData,detectData,startTime,endTime,outageStart,outageEnd):
        rawDataFrame = pd.DataFrame.from_csv(rawData)
        detectDataFrame = pd.DataFrame.from_csv(detectData)
        startIndex = (int)(chfetch.timeScaleConvert.convertGmtToEpoch(startTime))
        endIndex = (int)(chfetch.timeScaleConvert.convertGmtToEpoch(endTime))
        outageStartIndex = (int)(chfetch.timeScaleConvert.convertGmtToEpoch(outageStart))
        outageEndIndex = (int)(chfetch.timeScaleConvert.convertGmtToEpoch(outageEnd))
        epochTimeSeries = np.linspace(startIndex,endIndex,rawDataFrame.shape[0])
        detectWindowStart = int(np.abs((outageStartIndex - epochTimeSeries[0])/60))
        print 'WindowStart: ' + str(detectWindowStart)
        detectWindowEnd = int(np.abs((outageEndIndex - epochTimeSeries[0])/60))
        print 'WindowEnd: ' + str(detectWindowEnd)
        detectArray = {}
        for asn in detectDataFrame:
            if (np.sum(detectDataFrame[asn])!= 0):
                if np.sum(detectDataFrame[asn][detectWindowStart:detectWindowEnd]) > 0:
                    detect = 1
                else:
                    detect = 0
            else:
                detect = 0
            detectArray[asn] = detect
        return detectArray
    @staticmethod
    def asnQualify(basic_db_file):
        raw_data = pd.DataFrame.from_csv(basic_db_file)
        ineligible_asn = []
        eligible_asn = []
        for asn in raw_data:
            if (np.mean(raw_data[asn] < 10)):
                ineligible_asn.append(asn)
            else:
                eligible_asn.append(asn)
        return ineligible_asn
class detectStat(object):
    @staticmethod
    def outageAnalyze(event,detect_file,ineligible_asn,statFile):
        detect_df = pd.DataFrame.from_csv(detect_file)
        match = 0.0
        total = 0.0
        true_positive = 0.0
        true_negative = 0.0
        false_positive = 0.0
        false_negative = 0.0
        total_positive = 0.0

        for asn in detect_df:
            if asn in ineligible_asn:
                pass
            else:
                if (detect_df[asn]['MM']>=0):
                    if (detect_df[asn]['GT'] == detect_df[asn]['MM']):
                        match += 1.0
                        if (detect_df[asn]['GT'] > 0):
                            print "True Positive : " + str(asn)
                            true_positive += 1.0
                        else:
                            true_negative += 1.0
                    elif (detect_df[asn]['MM']>0):
                        print "False Positive : " + str(asn)
                        false_positive += 1.0
                        # print "False Positive : " + str(asn)
                    elif (detect_df[asn]['GT']>0):
                        print "False Negative : " + str(asn)
                        false_negative += 1.0
                    total += 1.0
            total_positive = true_positive + false_positive
            if(total_positive==0.0): total_positive = 1.0

        try:
            accuracy = float(match/total)
        except ZeroDivisionError:
            accuracy = -1

        try:
            precision = float(true_positive/total_positive)
        except ZeroDivisionError:
            precision = -1

        try:
            recall = float(true_positive/(true_positive+false_negative))
        except ZeroDivisionError:
            recall = -1

        try:
            f_score = float((2*(precision*recall))/(precision+recall))
        except ZeroDivisionError:
            f_score = -1

        try:
            sensitivity = true_positive/(true_positive+false_negative) #Same as Recall
        except ZeroDivisionError:
            sensitivity = -1

        try:
            specificity = true_negative/(true_negative+false_positive)
        except ZeroDivisionError:
            specificity = -1

        statDatadict = {}
        statDatadict['Event'] = event
        statDatadict['Accuracy'] = accuracy*100
        statDatadict['Precision'] = precision
        statDatadict['FScore'] = f_score
        statDatadict['Sensitivity'] = sensitivity*100
        statDatadict['Specificity'] = specificity*100
        statDatadict['TruePositive'] = true_positive
        statDatadict['TrueNegative'] = true_negative
        statDatadict['FalsePositive'] = false_positive
        statDatadict['FalseNegative'] = false_negative
        statDatadict['EligibleAsn'] = detect_df.shape[1] - np.shape(ineligible_asn)[0] -1
        with open(statFile,'a') as file:
            writer = csv.DictWriter(file,statDatadict.keys())
            writer.writeheader()
            writer.writerow(statDatadict)
