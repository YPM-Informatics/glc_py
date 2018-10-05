

#SEE https://zblesk.net/blog/csv-to-sqlite/ for getting sqldb built from this output

import sqlite3, csv, getopt, sys
import subprocess
from bottle import route, run, template, response, static_file, request

dbfile = None
inputTable = None
outputTable = None
countryHeader = 'country'
stateProvHeader = 'stateProvince'
countyHeader = 'county'
localityHeaders = []

@route('/geolocate')
def glc():
    return static_file('geolocate_embed_test.html', root='./')


@route('/save', method = 'POST')
def save_glc():
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'
    lid = request.params.get('lid')
    rid = request.params.get('rid')
    lat = request.params.get('lat')
    lon = request.params.get('lon')
    u = request.params.get('u')
    p = request.params.get('p')
    db = sqlite3.connect(dbfile)
    db.text_factory = str
    db.execute("INSERT INTO " + outputTable + " (latitude, longitude, uncertaintyRadiusMeters, uncertaintyPolygon, geolocate_LocalityID, geolocate_ResultID) VALUES(?,?,?,?,?,?)", (lat, lon, u, p, lid, rid))
    db.commit()
    db.close()
    #return lid + "::" + rid + "::" +  str(lat) + "::" + str(lon) + "::" +  str(u) + "::" + str(p)
    return "0"


def get_glc():
    modLocHeaders = []
    for l in localityHeaders:
        modLocHeaders.append("t1." + l)
    sql = 'SELECT ' + ', '.join(modLocHeaders) + ', t1.geolocate_locFieldUsed, t1.'+ countryHeader + ', t1.'+ stateProvHeader + ', t1.'+ countyHeader + ', t1.geolocate_Latitude, t1.geolocate_Longitude, t1.geolocate_ParsePattern, t1.geolocate_Precision, t1.geolocate_Score, t1.geolocate_UncertaintyRadiusMeters, t1.geolocate_UncertaintyPolygon, t1.geolocate_LocalityID, t1.geolocate_ResultID FROM '+ inputTable +' t1 LEFT JOIN '+ outputTable +' t2 ON t1.geolocate_LocalityID = t2.geolocate_LocalityID WHERE t2.ID IS NULL AND t1.geolocate_ResultID = "1" ORDER BY RANDOM() limit 1'
    db = sqlite3.connect(dbfile)
    db.text_factory = str
    row = db.execute(sql).fetchone()
    db.close()
    if (row):
        locHeaderIndex = 0
        startIndex = len(localityHeaders) - 1
        locFieldUsed = str(row[len(localityHeaders)]) 
        for l in localityHeaders:
            if  l == locFieldUsed:
                break;
            locHeaderIndex = locHeaderIndex + 1
        l = "k="+ str(locHeaderIndex) +"&locality="+ str(row[locHeaderIndex]) + '&country=' + row[startIndex + 2]+ '&state=' + row[startIndex + 3] + '&county=' + row[startIndex + 4] + '&lid=' + (str)(row[startIndex + 12]) + '&rid=' + (str)(row[startIndex + 13])
        result = l +'&'+  'points=' + '|'.join([(str)(row[startIndex + 5]), (str)(row[startIndex + 6]), (str)(row[startIndex + 7]), (str)(row[startIndex + 8]) + '('+ (str)(row[startIndex + 9]) +')' ,(str)(row[startIndex + 10])]) 
    else:
        result = "locality=End of Data Reached";
    return result


@route('/getrec', method='GET')
def get_next():
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'
    return(get_glc())



def showHelp():
    print('glcserver.py -i <input_dbfile> -t <input_table> -o <output_table> [options]')
    print('-c <country field> default: country')
    print('-s <state field> default: stateProvince')
    print('-a <county field> default: county')
    print('-l <locality field> comma separated list of localty firlds to try in order of preference default: locality')
    sys.exit()

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'i:t:o:c:s:a:l:h')
    except getopt.GetoptError as err:
        print(err)
        print('invalid args, for help: geolocate.py -h')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            showHelp()
        elif opt == '-i':
            dbfile = arg
        elif opt == '-t':
            inputTable = arg
        elif opt == '-o':
            outputTable = arg
        elif opt == '-c':
            countryHeader = arg
        elif opt == '-s':
            stateProvHeader = arg
        elif opt == '-a':
            countyHeader = arg
        elif opt == '-l':
            localityHeaders.extend(arg.split(','))
        else:
            assert False, "unhandled option"   

    if len(localityHeaders) == 0:
        localityHeaders.append('locality')
    
    if dbfile is None or inputTable is None  or outputTable is None:
        showHelp()

    db = sqlite3.connect(dbfile)
    db.text_factory = str
    db.execute("CREATE TABLE IF NOT EXISTS " + outputTable + " (id INTEGER PRIMARY KEY AUTOINCREMENT, latitude text, longitude text, uncertaintyRadiusMeters text, uncertaintyPolygon text, geolocate_LocalityID, geolocate_ResultID)")
    db.close()

    run(host='localhost', port=8080, debug=True, reloader=True)