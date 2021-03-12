#!/usr/bin/env python3.7
# ###
# python 3.7
# simple script to run a csv through GEOLocate webservices
# postprocess with csv-to-sqlite -f out.csv -o out.db --no-types 
# ###

import os, sys, getopt, urllib, json, csv, time
from collections import namedtuple
import urllib.request
import sqlite3

maxInt = sys.maxsize
decrement = True

while decrement:
    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True


def str2bool(s):
    s = s.lower()
    if s == "true":
        return True
    elif s == "false":
        return False
    else:
        raise ValueError

class GeolocateResult:
    def __init__(self, feature):
        self.latitude = feature['geometry']['coordinates'][1]
        self.longitude = feature['geometry']['coordinates'][0]
        self.uncertaintyRadiusMeters = feature['properties']['uncertaintyRadiusMeters']
        self.uncertaintyPolygon = feature['properties']['uncertaintyPolygon']
        self.precision = feature['properties']['precision']
        self.score = feature['properties']['score']
        self.parsePattern = feature['properties']['parsePattern']
        self.displacedDistanceMiles = feature['properties']['displacedDistanceMiles']
        self.displacedHeadingDegrees = feature['properties']['displacedHeadingDegrees']
        self.debug = feature['properties']['debug']

class GeolocateResultSet:
    def __init__(self, jsonResult):
        try:
            data = json.loads(jsonResult)
        except:
            Ex = ValueError()
            Ex.strerror = "invalid JSON returned"
            raise(Ex)
        self.engineVersion = data['engineVersion']
        self.numResults = data['numResults']
        self.results = [] 
        self.source = None;
        i = 0
        if (self.numResults > 0):
            for feature in data['resultSet']['features']:
                print(feature)
                self.results.append(GeolocateResult(feature))
                

class Geolocate:
    def __init__(self, cacheDB=None, debug=False):
        self.cacheDB=None
        self.endpoint="http://www.geo-locate.org/webservices/geolocatesvcv2/glcwrap.aspx"
        self.debug = debug
        if (cacheDB != None):
            self.cacheDB = sqlite3.connect(cacheDB)
            self.cacheDB.text_factory = str
            self.cacheDB.execute('''CREATE TABLE IF NOT EXISTS reqres (id INTEGER PRIMARY KEY AUTOINCREMENT, request text, response text)''')
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        if self.cacheDB != None:
            self.cacheDB.close()
    def log(self, s):
        if self.debug:
            print(s)
    def georef(self, locality, country, stateProv, county, hwyX=True, enableH2O=True, doUncert=True, doPoly=False, displacePoly=False, languageKey=0):
        data = None
        p = {'country': country, 'locality': locality, 'state': stateProv, 'county':county, 'hwyX':str(hwyX),'enableH2O':str(enableH2O), 'doUncert':str(doUncert), 'doPoly':str(doPoly), 'displacePoly': str(displacePoly), 'languageKey':str(languageKey), 'fmt':'json'}
        params = urllib.parse.urlencode(p).encode('utf8')
        db_row = None
        source = self.endpoint
        if self.cacheDB != None:
            db_row = self.cacheDB.execute('SELECT response from reqres where request =?', [params]).fetchone()
        if (db_row != None):
            self.log("Using HTTP Cache")
            resultJson = db_row[0]
            source = "cache"
        else:
            with urllib.request.urlopen(self.endpoint,params) as r:
                resultJson = r.read()
            if self.cacheDB != None:
                self.cacheDB.execute('INSERT into reqres VALUES(null,?,?)', [params, resultJson])
                self.cacheDB.commit()
            self.log(resultJson)
        try:
            r = GeolocateResultSet(resultJson)
            r.source = source
            return(r)
        except ValueError as e:
            if self.cacheDB != None:
                self.cacheDB.close()
            self.log(e.strerror)
            raise e

def showHelp():
    print('geolocate.py -i <inputfile> -o <outputfile> [options]')
    print('-c <country field> default: country')
    print('-s <state field> default: stateProvince')
    print('-a <county field> default: county')
    print('-l <locality field> comma separated list of locality fields to try in order of preference default: locality')
    print('-t <seconds between requests> default: .6')
    print('-n <num records to process> default: entire input file')
    print('-v verbose output of raw json response to terminal')
    print('-1, -- firstOnly outputs first result only for each record')
    print('--cache=<cache file>')
    print('--hwX=<[true] of false>')
    print('--enableH2O=<[true] of false>')
    print('--doUncert=<[true] of false>')
    print('--doPoly=<true of [false]>')
    print('--displacePoly=<true or [false]>')
    print('--languageKey=<[0], 1, 2, 3 or 4>')
    sys.exit()



if __name__ == "__main__":
    
    inFile =  None 
    outFile = None 
    outFirstOnly = False
    t = .6  # time delay in seconds
    countryHeader = 'country'
    stateProvHeader = 'stateProvince'
    countyHeader = 'county'
    localityHeaders = []
    glc_options= {'hwyX':'true','enableH2O':'true', 'doUncert':'true', 'doPoly':'false', 'displacePoly': 'false', 'languageKey':'0', 'fmt':'json'}
    verbose = False
    nmax = -1
    dbfile = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'i:o:c:s:a:l:t:n:v1h',['cache=', 'firstOnly', 'hwyX=','enableH2O=', 'doUncert=', 'doPoly=', 'displacePoly=', 'languageKey='])
    except getopt.GetoptError as err:
        print(err)
        print('invalid args, for help: geolocate.py -h')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            showHelp()
        elif opt == '-i':
            inFile = arg
        elif opt == '-o':
            outFile = arg
        elif opt == '-c':
            countryHeader = arg
        elif opt == '-s':
            stateProvHeader = arg
        elif opt == '-a':
            countyHeader = arg
        elif opt == '-l':
            localityHeaders.extend(arg.split(','))
        elif opt in ('-1','--firstOnly'):
            outFirstOnly = True
        elif opt == '-t':
            t = float(arg)
        elif opt == '-n':
            nmax = int(arg)
        elif opt == '-v':
            verbose = True
        elif opt == '--cache':
            dbfile = arg
        elif opt == '--hwyX':
           glc_options['hwyX'] = str2bool(arg)
        elif opt == '--enableH2O':
           glc_options['enableH2O'] = str2bool(arg)
        elif opt == '--doUncert':
           glc_options['doUncert'] = str2bool(arg)
        elif opt == '--doPoly':
           glc_options['doPoly'] = str2bool(arg)
        elif opt == '--displacePoly':
           glc_options['displacePoly'] = str2bool(arg)
        elif opt == '--languageKey':
           glc_options['languageKey'] = int(arg)
        else:
            assert False, "unhandled option"   

    if len(localityHeaders) == 0:
        localityHeaders.append('locality')

    if inFile is None or outFile is None:
        showHelp()

    with Geolocate(debug=verbose, cacheDB=dbfile) as glc:
        with open(inFile, 'rt', encoding='utf8') as csvfile0:
            reader = csv.DictReader(csvfile0)
            header = reader.fieldnames
            header2 = reader.fieldnames
            header2.extend(['geolocate_LocalityID', 'geolocate_ResultID','geolocate_Latitude', 'geolocate_Longitude', 'geolocate_UncertaintyRadiusMeters','geolocate_UncertaintyPolygon', 
                            'geolocate_Score', 'geolocate_Precision', 'geolocate_ParsePattern','geolocate_locFieldUsed','geolocate_NumResults'])
            startAt = 0
            if (os.path.isfile(outFile)):
                 with open(outFile, 'rt', encoding='utf8') as tmp:
                    tmp_reader = csv.DictReader(tmp)
                    for row in tmp_reader:
                        startAt = startAt +1;
                    print('Existing file detected, starting recovery at record no.', startAt)
                    time.sleep(5)
            with open(outFile, 'a+', encoding='utf8', newline='') as csvfile1:
                writer = csv.DictWriter(csvfile1, fieldnames=header)
                if startAt == 0:
                    writer.writeheader()
                n = 0
                for row in reader:
                    n = n+1
                    #print(n)
                    if (nmax > 0 and n > nmax):
                        break;
                    if (n > startAt):
                        print(n)
                        row['geolocate_LocalityID'] = n
                        for locHeader in localityHeaders:
                            print(row[locHeader])
                            resultSet = glc.georef(row[locHeader],row[countryHeader],row[stateProvHeader], row[countyHeader], 
                                           hwyX=glc_options['hwyX'], enableH2O=glc_options['enableH2O'], doUncert=glc_options['doUncert'],
                                           doPoly=glc_options['doPoly'],displacePoly=glc_options['displacePoly'],languageKey=glc_options['languageKey'])
                    
                            if resultSet.source != "cache":
                                time.sleep(t)
                            print("No. Results: " + str(resultSet.numResults))
                            row['geolocate_NumResults'] = resultSet.numResults
                            if (resultSet.numResults > 0):
                                row['geolocate_locFieldUsed'] = locHeader
                                break;
                        i = 0
                        if (resultSet.numResults > 0):
                            for res in resultSet.results:
                                i += 1;
                                row['geolocate_ResultID'] = i
                                row['geolocate_Latitude'] = res.latitude                            
                                row['geolocate_Longitude'] = res.longitude
                                row['geolocate_UncertaintyRadiusMeters'] = res.uncertaintyRadiusMeters
                                row['geolocate_UncertaintyPolygon'] = res.uncertaintyPolygon
                                row['geolocate_Precision'] = res.precision
                                row['geolocate_Score'] = res.score
                                row['geolocate_ParsePattern'] = res.parsePattern
                                if outFirstOnly == False:
                                        writer.writerow(row)
                                elif i == 1:
                                        writer.writerow(row)
                                print(row)
                        else:
                                writer.writerow(row)
                        print('***************')
                        

