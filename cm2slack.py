#!/usr/bin/env python

import json
import argparse
import os
import socket
import urllib2

# Cloudera Manager passes alerts to this script as a path to a file containing JSON with the alert data
parser = argparse.ArgumentParser(description="Processes Cloudera Manager alerts in JSON file.")
parser.add_argument('alertFile', metavar='<file name>', type=file, nargs=1)

args = parser.parse_args()

# parse the JSON containing the alerts -- there could be multiple
alertList = json.load(args.alertFile[0])

# Slack has a REST interface to submit messages -- using this requires enabling "webhooks" for a channel, and this will provide a "token" that must be on the URL to the REST service
slackToken=''

# proxy definition
#os.putenv("http_proxy", "http://proxy.internal:8080")
#os.putenv("https_proxy", "http://proxy.internal:8080")

status_map={ "GREEN":  { "colour": "good", "status_text": "Good" },
             "YELLOW": { "colour": "warning", "status_text": "Concerning" },
             "RED": { "colour": "danger", "status_text": "Bad" }}

def generate_alert_definition(alert):
    alert_status = alert["body"]["alert"]["attributes"]["CURRENT_HEALTH_SUMMARY"][0]
    attachment = {
        "title": alert["body"]["alert"]["attributes"]["ALERT_SUMMARY"][0],
        "title_link": alert["body"]["alert"]["source"],
        "text": alert["body"]["alert"]["content"],
        "fields": [
            {
                "title": "Test Name",
                "value": alert["body"]["alert"]["attributes"]["HEALTH_TEST_NAME"][0],
                "short": False
            },
            {
                "title": "Status",
                "value": status_map[alert_status]["status_text"],
                "short": True
            },
            {
                "title": "Service",
                "value": alert["body"]["alert"]["attributes"]["SERVICE"][0],
                "short": True
            }
         ],
        "color": status_map[alert_status]["colour"],
        "ts": str(alert["body"]["alert"]["timestamp"]["epochMs"])[:-3],
        "footer": "Cloudera Manager on {}".format(alert["body"]["alert"]["attributes"]["CLUSTER"][0])
    }
    return attachment


# loop through the alerts in the JSON and send each separately to Slack. Note that stdout goes to the cloudera manager log(s).
alert_dict = { "attachments": [generate_alert_definition(alert) for alert in alertList] }

req = urllib2.Request('https://hooks.slack.com/services/{}'.format(slackToken))
req.add_header('Content-Type', 'application/json')

response = urllib2.urlopen(req, json.dumps(alert_dict))
