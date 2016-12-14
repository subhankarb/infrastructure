#!/bin/bash
echo running file for feed $FEED and date $EVENTDATE
export
echo python3 ./ETL.py -f $FEED -d $EVENTDATE
cd /app/ && python3 ./ETL.py -f $FEED -d $EVENTDATE
