import sys
import os
from flask import request
from flask import Flask, jsonify
from ConfigParser import SafeConfigParser
import SeviceConstants
from flask import abort
import SqoopUtility
import LocalToS3
import HdfsToS3
from LogSetup import logger
from flask_basicauth import BasicAuth


CONFIGURATION_FILE =os.path.normpath(os.path.dirname(os.path.realpath(__file__)))+'/settings.conf'
parser = SafeConfigParser()
parser.read(CONFIGURATION_FILE)
host = parser.get("servicesettings","host")
port = parser.get("servicesettings","port")
servicename=parser.get("servicesettings","servicename")
response={"success":"False"}

app = Flask(servicename)
basic_auth = BasicAuth(app)


app.config['BASIC_AUTH_USERNAME'] = 'admin'
app.config['BASIC_AUTH_PASSWORD'] = 'admin'


@app.route('/dataexportservice/export', methods=['POST'])
@basic_auth.required
def exportToS3():

    if request.get_json == {} or "export_type" not in request.json.keys() :
        logger.error(SeviceConstants.REQUIRED_PARAMETER_MISSING)
        return abort(400, SeviceConstants.REQUIRED_PARAMETER_MISSING)

    if request.json["export_type"] == "dbexport":
        response= SqoopUtility.runSqoop(request.json)
    elif request.json["export_type"] == "hdfsToS3":
        response=HdfsToS3.runHdfsTOS3(request.json)

    elif request.json["export_type"]=="localToS3":
        response = LocalToS3.runLocalTos3Upload(request.json)
    else:
        logger.error(SeviceConstants.REQUIRED_PARAMETER_MISSING)
        return abort(400, SeviceConstants.INVALID_INPUT)
    return jsonify(response)


if __name__ == '__main__':
    logger.info("Starting service" )
    app.run(host=host, port=int(port), debug=True)
