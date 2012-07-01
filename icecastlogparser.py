#!/usr/bin/python
# icecastServerLogParser.py
# Alejandro Ferrari <support@wmconsulting.info>
# version 1.0

"""
Parser for ICECAST server log output, of the form:

190.49.30.2 - - [25/Jun/2012:04:50:59 -0300] 
"GET /Retromix_64.mp3?1340608279543.mp3 HTTP/1.1" 200 19143936 
"http://reproductor.cienradios.com.ar/player/Flash/" 
"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.5 (KH" 2385

You can then break it up as follows:
IP ADDRESS - -
Server Date / Time [SPACE]
"GET /path/to/stream
HTTP/Type Request"
Success Code
Bytes Sent To Client
Referer
Clien Software/User-Agent
Session Duration Time
"""

from pyparsing import alphas,nums, dblQuotedString, Combine, Word, Group, delimitedList, Suppress, removeQuotes
import string
import glob
import sys
import MySQLdb
import pygeoip
import time
from datetime import datetime
from datetime import timedelta

#################################################
# Configurations
#################################################
# glob supports Unix style pathname extensions
# Here need to put the streamName you need parse
python_files = glob.glob('*.log')

# Put the correct path to your .DAT GeoIP DB
gi  = pygeoip.GeoIP('/usr/share/GeoIP/GeoIP.dat')
gic = pygeoip.GeoIP('/usr/share/GeoIP/GeoLiteCity.dat')

# DB Params
db_host = localhost
db_user = icecast
db_passwd = pass
db_name  = icecast_stats

#################################################
# Dont modify below this line
#################################################

try:
     conn = MySQLdb.connect (host = "db_host", user = "db_user", passwd = "db_passwd", db = "db_name")
except MySQLdb.Error, e:
     print "Error %d: %s" % (e.args[0], e.args[1])
     sys.exit (1)

def getCmdFields( s, l, t ):
    t["method"],t["requestURI"],t["protocolVersion"] = t[0].strip('"').split()

logLineBNF = None
def getLogLineBNF():
    global logLineBNF
    
    if logLineBNF is None:
        integer = Word( nums )
        ipAddress = delimitedList( integer, ".", combine=True )
        
        timeZoneOffset = Word("+-",nums)
        month = Word(string.uppercase, string.lowercase, exact=3)
        serverDateTime = Group( Suppress("[") + Combine( integer + "/" + month + "/" + integer + ":" + integer + ":" + integer + ":" + integer ) + timeZoneOffset + Suppress("]") )
	              
        logLineBNF = ( ipAddress.setResultsName("ipAddr") + 
                       Suppress("-") +
                       ("-" | Word( alphas+nums+"@._" )).setResultsName("auth") +
                       serverDateTime.setResultsName("timestamp") + 
                       dblQuotedString.setResultsName("cmd").setParseAction(getCmdFields) +
                       (integer | "-").setResultsName("statusCode") + 
                       (integer | "-").setResultsName("numBytesSent")  + 
                       dblQuotedString.setResultsName("referer").setParseAction(removeQuotes) +
                       dblQuotedString.setResultsName("userAgent").setParseAction(removeQuotes) +
		       (integer | "-").setResultsName("numDurationTime"))
    return logLineBNF
    
for file_name in sorted(python_files):
    with open(file_name) as f:
        for line in f:
	    if not line: continue
	    fields = getLogLineBNF().parseString(line)
	    countryCode = gi.country_code_by_addr(fields.ipAddr)
	    streamName = fields.requestURI.strip('/').split('?')
	    datetime_end = datetime.strptime(fields.timestamp[0],"%d/%b/%Y:%H:%M:%S")
	    datetime_start = datetime_end - timedelta(seconds=int(fields.numDurationTime))

            # prepare a cursor object using cursor() method
	    cursor = conn.cursor()
	    try:
		# Execute the SQL command
		cursor.execute("INSERT INTO icecast_logs (datetime_start, datetime_end, ip, country_code, mount, status_code, duration, sent_bytes, agent, referer, server, user, pass) \
				VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(datetime_start, datetime_end, fields.ipAddr, countryCode, streamName[0], fields.statusCode, fields.numDurationTime, fields.numBytesSent, fields.userAgent, fields.referer, fields.serverName, fields.userName, fields.password))
		# Commit your changes in the database
		conn.commit()
	    except MySQLdb.Error, e:
		# Rollback in case there is any error
		conn.rollback()
		print "An error has been passed. %s" %e

	    cursor.close()

conn.close ()

