__author__ = 'lenord'
import pandas as pd
import chartanalysis as chanalysis
import detectalgo
import numpy as np
import csv
from requests import Session
import datetime
import json

class outageEvent(object):
    cookie = '4habk8k8vf1b9j1aoeiso6g570'
    statLog = 'results'+'\\'+ datetime.datetime.now().strftime("%Y%m%d%H%M") + '_log.csv'

    def __init__(self,event,startTime,endTime,outageStart,outageEnd,outagePeriod):
        """
        Constructor to initialize Outage Event specific details
        :param event: Outage Event
        :param startTime: Log start time
        :param endTime: Log end time
        :param outageStart: Anticipated outage start time
        :param outageEnd: Anticipated outage end time
        :return: None
        """
        self.event = event
        self.startTime = startTime
        self.endTime = endTime
        self.outageStart = outageStart
        self.outageEnd = outageEnd
        self.outagePeriod = outagePeriod
        self.asnQualified = 'eventData' + '\\' + event + '\\' + outagePeriod + '\\asn.txt'
        self.groundTruth = 'groundTruth' + '\\' + event + '_gt.csv'
        self.rawData = 'results' + '\\' + event + '\\' + event + '_db.csv' #JUST CHECKING with DB1
        self.detectData = 'results' + '\\' + event + '\\' + event + '_detect.csv'
        self.rawDetect = 'results' + '\\' + event + '\\' + event + '_gt_detect.csv'
        # self.statLog = 'results' + '\\' + event + '\\' + event + '_log.csv'
        self.asnList = pd.read_table(self.asnQualified)
class timeScaleConvert(object):
    @staticmethod
    def convertGmtToEpoch(dateTime):
        '''
        Convert given Date&Time to Epoch format to query from ChartHouse
        :param dateTime: Date&Time to be converted (Format : %m-%d-%y %H:%M:%S)
        :return: epoch format of input Date&Time
        '''
        epoch = datetime.datetime(1970, 1, 1)
        epochDateTime = datetime.datetime.strptime(dateTime, "%m-%d-%y %H:%M:%S")
        secondsFromEpoch = (epochDateTime - epoch).total_seconds()
        return int(secondsFromEpoch)
class caidaDetect(object):
    @staticmethod
    def caidaDetectUrlExpression(asn):
        '''
        For a given asn, this function generates URL expression to query ChartHouse with data filtered
        using Caida Detect Algorithm
        :param asn: ASN to be queried for Detection
        :return: expression to be used in URL query
        '''
        expr="""{"type":"function","func":"removeBelowValue","args":[{"type":"function","func":"diffSeries","args":
        [{"type":"function","func":"timeShift","args":[{"type":"function","func":"transformNull","args":
        [{"type":"function","func":"removeBelowValue","args":[{"type":"function","func":"diffSeries","args":
        [{"type":"function","func":"movingMinimum","args":
        [{"type":"path","path":"darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip"},
        {"type":"constant","value":120}]},{"type":"function","func":"diffSeries","args":
        [{"type":"function","func":"movingMedian","args":
        [{"type":"path","path":"darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip"},
        {"type":"constant","value":20}]},{"type":"function","func":"movingMinimum","args":
        [{"type":"path","path":"darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip"},
        {"type":"constant","value":20}]}]}]},{"type":"constant","value":0}]},{"type":"constant","value":0}]},
        {"type":"constant","value":"20min"}]},{"type":"function","func":"keepLastValue","args":
        [{"type":"path","path":"darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip"}]}]},
        {"type":"constant","value":0}]}"""
        return expr
    @staticmethod
    def caidaDetectJsonExpression(asn):
        '''
        For a given asn, this function generates query expression to used to retrieve the filtered data from the
        downloaded JSON
        :param asn: ASN to be queried for Detection
        :return: expression to be used in JSON query
        '''
        expr="""removeBelowValue(diffSeries(transformNull(removeBelowValue(diffSeries(movingMinimum(darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip,120),diffSeries(movingMedian(darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip,20),movingMinimum(darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip,20))), 0),0),keepLastValue(darknet.ucsd-nt.non-spoofed.routing.asn."""+asn+""".uniq_src_ip)), 0)"""
        return expr
class DatabaseBuilder(object):
    @staticmethod
    def rawDatabaseBuild(asnList,startTime,endTime,cookie,databaseFile):
        '''
        Create Database using Raw data queried from Charthouse
        :param asnList: List of ASN to be queried
        :param startTime: Start Time of the query window
        :param endTime: End Time of the query window
        :param cookie: Cookie to be used to open session
        :param databaseFile: Destination DataBase File Name
        :return: None
        '''
        ipDataFrame = pd.DataFrame()
        print 'Building Raw Database'
        asnRawChart = chartFetcher(cookie,startTime,endTime)
        for asn in asnList['ASN']:
            asnRawChart.chartUrlSetup(str(asn))
            uniqueSourceIpData = asnRawChart.rawChartDataGet()
            ipDataFrame[asn] = uniqueSourceIpData
            print asn
        ipDataFrame.to_csv(databaseFile)
        print 'Database created'
    @staticmethod
    def filteredDatabaseBuild(asnList,startTime,endTime,cookie,databaseFile):
        '''
        Create Database using Filtered Data Queried from Charthouse
        :param asnList: List of ASN to be queried
        :param startTime: Start Time of the query window
        :param endTime: End Time of the query window
        :param cookie: Cookie to be used to open session
        :param databaseFile: Destination DataBase File Name
        :return: None
        '''
        averageIpThreshold = 10
        filterIpDataFrame = pd.DataFrame()
        print 'Building Filtered Database'
        asnFilterChart = chartFetcher(cookie,startTime,endTime)
        for asn in asnList['ASN']:
            asnFilterChart.chartUrlSetup(str(asn))
            uniqueSourceIpData = asnFilterChart.rawChartDataGet()
            if(np.mean(uniqueSourceIpData) > averageIpThreshold):
                filterIpDataFrame[asn] = asnFilterChart.filterChartDataGet()
            else:
                filterIpDataFrame[asn] = np.zeros(np.shape(uniqueSourceIpData)[0])
            print asn
        filterIpDataFrame.to_csv(databaseFile)
        print 'Database created'
    @staticmethod
    def groundDetectGroupDatabaseBuild(rawDatabaseFile,filterDatabaseFile,startTime,endTime,outageStart,outageEnd,
                                       groundTruthFile,rawDetectFile):
        '''
        For a given event parameters, check if the outage in the anticipated period has been detected. Build database
        with Ground truth and Detection Data for analysis purposes
        :param rawDatabaseFile: CSV containing Raw Data that was filtered
        :param filterDatabaseFile: CSV containing Filtered Data
        :param startTime: start time of the event
        :param endTime: end time of the event
        :param outageStart: anticipated outage start time
        :param outageEnd: anticipated outage end time
        :param groundTruthFile: CSV containing Ground Truth data
        :param rawDetectFile: CSV where Ground truth and Detection Data will be stored
        :return: None
        '''
        detectionDict = chanalysis.outageDetect.detectAnyOutage(rawDatabaseFile,filterDatabaseFile,startTime,endTime,outageStart,outageEnd)
        groundTruth = csv.reader(open(groundTruthFile,'r'),dialect='excel')
        groundTruthDict = {}
        for row in groundTruth:
            groundTruthDict[row[0]] = {}
            groundTruthDict[row[0]]['GT'] = row[2]

        for asn in detectionDict.keys():
            if asn in groundTruthDict:
                groundTruthDict[asn]['MM'] = detectionDict[asn]

        groundTruthDataFrame = pd.DataFrame.from_dict(groundTruthDict)
        groundTruthDataFrame.to_csv(rawDetectFile)
    @staticmethod
    def detectDatabaseBuild(rawDatabaseFile,detectDatabaseFile,algoChoice):
        rawDataFrame = pd.DataFrame.from_csv(rawDatabaseFile)
        detectDataFrame = pd.DataFrame()
        for asn in rawDataFrame:
            uniqueSourceIpData = np.array(rawDataFrame[str(asn)])
            if (np.mean(uniqueSourceIpData) > 10):
                if(algoChoice == 'caida'):
                    detectIpData = detectalgo.algoCaidaDetect(uniqueSourceIpData)
                    detectDataFrame[asn] = detectIpData
                elif(algoChoice == 'bayes'):
                    detectIpData = detectalgo.bayesChangePoint(uniqueSourceIpData)
                    detectDataFrame[asn] = detectIpData
                else:
                    pass
            else:
                detectDataFrame[asn] = np.zeros(uniqueSourceIpData.shape[0])
        detectDataFrame.to_csv(detectDatabaseFile)
class chartFetcher:
    def __init__(self,cookie,startTime,endTime):
        '''
        Constructor to setup Query window and authentication cookie
        :param cookie: Cookie to authenticate secure data query
        :param startTime: Start time of the query window
        :param endTime: End time of the query window
        :return: None
        '''
        self.cookie = cookie
        self.startTime = startTime
        self.endTime = endTime
    def chartUrlSetup(self,asn):
        '''
        Setup basic parameters for building URL query to fetch data from Charthouse
        :param asn: ASN to be queried for
        :return: None
        '''
        self.charthouseSession = Session()
        self.charthouseSession.cookies["charthouse_session"] = self.cookie
        self.startEpoch = timeScaleConvert.convertGmtToEpoch(self.startTime)
        self.endEpoch = timeScaleConvert.convertGmtToEpoch(self.endTime)
        if (asn == 0): self.asnExpression = 'overall'
        else: self.asnExpression = "routing.asn." + asn
        self.rawExpression = 'darknet.ucsd-nt.non-spoofed.' + self.asnExpression + '.uniq_src_ip'
        self.filterExpression = caidaDetect.caidaDetectUrlExpression(asn)
        self.rawPayload = {
            'from': self.startEpoch,
            'until': self.endEpoch,
            'expression': self.rawExpression,
            'annotate':'true',
            'human':'true'
        }
        self.filterPayload = {'from': self.startEpoch,'until': self.endEpoch,'expression': self.filterExpression,'annotate':'true','human':'true'}
        self.filterJsonExpression = caidaDetect.caidaDetectJsonExpression(asn)
    def rawChartDataGet(self):
        """
        Queries raw data from Charthouse for a given object with parameters initialized
        :return: List of Number of Unique source IPs advertised per minute by the ASN
        """
        self.rawChartData = self.charthouseSession.get("https://charthouse.caida.org/data/ts/json", params=self.rawPayload)
        self.rawChartJson = json.loads(self.rawChartData.content)
        self.uniqueSourceIpData = self.rawChartJson["data"]["series"][self.rawExpression]["values"]
        self.uniqueSourceIpData= [0 if numIp is None else numIp for numIp in self.uniqueSourceIpData]
        return self.uniqueSourceIpData
    def filterChartDataGet(self):
        '''
        Queries filtered data from Charthouse for a given object with parameters initialized
        :return: List of Filtered Number of Unique source IPs advertised per minute by the ASN
        '''
        self.filterChartData = self.charthouseSession.get("https://charthouse.caida.org/data/ts/json", params=self.filterPayload)
        self.filterChartJson = json.loads(self.filterChartData.content)
        self.filterUniqueIpData = self.filterChartJson["data"]["series"][self.filterJsonExpression]["values"]
        # self.filterUniqueIpData = [0 if numIp is None else 2*max(self.uniqueSourceIpData) for numIp in self.filterUniqueIpData]
        return self.filterUniqueIpData
def main():
    '''

    '''
    cnci = outageEvent('cnci','08-17-09 00:00:00','08-17-09 23:59:00', '08-17-09 18:00:00','08-17-09 18:40:00','1080-1120')
    # afnog = outageEvent('afnog','05-03-09 00:00:00','05-03-09 23:59:00','05-03-09 12:00:00','05-03-09 13:00:00','720-780')
    # czech = outageEvent('czech','02-16-09 00:00:00','02-16-09 23:59:00','02-16-09 16:20:00','02-16-09 17:20:00','980-1040')
    # rd = outageEvent('rd','08-27-10 00:00:00','02-16-09 23:59:00','08-27-10 08:30:00','08-27-10 09:30:00','510-570')
    # jun = outageEvent('jun','11-07-11 00:00:00','11-07-11 23:59:00','08-27-10 08:30:00','11-07-11 15:00:00','840-900')
    # eventList = [cnci,afnog,czech,rd,jun]
    eventList = [cnci]

    # Rebuild Raw Database only IF parameters have changed #
    rawRebuildChoice = raw_input('Do you want to Rebuild your Raw Database? (Yes/No):')
    if(rawRebuildChoice=='Yes'):
        for eventName in eventList:
            print eventName.event
            DatabaseBuilder.rawDatabaseBuild(eventName.asnList,eventName.startTime,eventName.endTime,
                                             eventName.cookie,eventName.rawData)

    # Rebuild Charthouse Filtered Database
    filterRebuildChoice = raw_input('Do you want to Rebuild ChartHouse Filtered Database? (Yes/No): ')
    if(filterRebuildChoice=='Yes'):
        for eventName in eventList:
            print eventName.event
            DatabaseBuilder.filteredDatabaseBuild(eventName.asnList,eventName.startTime,eventName.endTime,
                                                  eventName.cookie,eventName.detectData)
            DatabaseBuilder.groundDetectGroupDatabaseBuild(eventName.rawData,eventName.detectData,eventName.startTime,
                                                           eventName.endTime,eventName.outageStart,eventName.outageEnd,
                                                           eventName.groundTruth,eventName.rawDetect)
            print 'Analyzing Detection Efficiency :'
            for eventName in eventList:
                disqualifiedAsn = chanalysis.outageDetect.asnQualify(eventName.rawData)
                chanalysis.detectStat.outageAnalyze(eventName.rawDetect,disqualifiedAsn,eventName.statLog)

    # Build and Analyze Custom Detection Algorithm
    else:
        customDetectChoice = raw_input('Do you want to build Custom Detection Database? (Yes/No): ')
        if(customDetectChoice=='Yes'):
            algoChoice = raw_input('Choose the algorithm to use (caida): ')
            print 'Algorithm :' + algoChoice
            for eventName in eventList:
                print eventName.event
                DatabaseBuilder.detectDatabaseBuild(eventName.rawData,eventName.detectData,algoChoice)
                DatabaseBuilder.groundDetectGroupDatabaseBuild(eventName.rawData,eventName.detectData,eventName.startTime,
                                                               eventName.endTime,eventName.outageStart,eventName.outageEnd,
                                                               eventName.groundTruth,eventName.rawDetect)
            print 'Analyzing Detection Efficiency'
            for eventName in eventList:
                disqualifiedAsn = chanalysis.outageDetect.asnQualify(eventName.rawData)
                chanalysis.detectStat.outageAnalyze(str(eventName.event),eventName.rawDetect,disqualifiedAsn,eventName.statLog)
    print 'Detection complete'
if __name__ == '__main__':
    main()


