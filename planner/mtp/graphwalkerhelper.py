import requests
import ipaddress
import json

class GraphWalkerHelper:


    @staticmethod
    def validate_ip(ip: str) -> bool:
        try:
            ipaddress.ip_address(ip)
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_port(port: int) -> bool:
        return 1 <= port <= 65535

    @staticmethod
    def validateJSON(jsonData):
        try:
            json.loads(jsonData)
            return True
        except ValueError as err:
            print("validateJSON:",err)
            return False

    def __init__(self,ip: str,port: int):

        if not self.validate_ip(ip=ip) or not self.validate_port(port=port):
            raise Exception("GraphWalkerHelper: ip or port not valid")

        self.service_status = False
        self.ip = ip
        self.port = port
        self.endpoints = {
            # Graphwalker-REST API-endpoints
            # GET Requests
            "getNext": f"http://{ip}:{port}/graphwalker/getNext",
            "hasNext": f"http://{ip}:{port}/graphwalker/hasNext",
            "getStat": f"http://{ip}:{port}/graphwalker/getStatistics",
            # POST Requests
            "loadModel": f"http://{ip}:{port}/graphwalker/load",
            # PUT Request
            "restartTestGen": f"http://{ip}:{port}/graphwalker/restart",
            "failTest": f"http://{ip}:{port}/graphwalker/fail/Teste%20abortado",
        }
        self.model = {}


    def isGWOnline(self):
        request = requests.get(self.endpoints["getNext"])
        print(request.status_code)
        if (request.status_code == 200):
            print("GraphWalkerHelper: GraphWalker is online")
            self.service_status = True
        else:
            print("GraphWalkerHelper: Service is F0KING DEAD, MATE!")

    def load_model_to_graphwalker(self):

        if self.model and self.service_status:
            # the Graphwalker Rest API recieves a PLAIN/TEXT, but the text must be a JSON OBJECT
            r = requests.post(self.endpoints["loadModel"], json.dumps(self.model))
            return r.json()
        else:
            return {"Error":"Model or Service not available"}

    def abortTestCaseGen(self):
        r = requests.put(self.endpoints["failTest"])
        return r.text

    def restartTest(self):
        r = requests.put(self.endpoints["restartTestGen"])
        return r.text

    def getNext(self):
        r = requests.get(self.endpoints["getNext"])
        return r.text

    def hasNext(self):
        # determina se ainda é necessário continuar com a criação do caso de teste
        # quando a condição de parada do gerador é alcançada ele responde negativamente.
        r = requests.get(self.endpoints["hasNext"])
        return r.text