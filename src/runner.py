from pprint import pprint
import httpx
import orjson


class Runner:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.client = httpx.AsyncClient(http2=True, timeout=1.0)

    def convert_rapid_options(self, options):
        rapid_options = []
        for key, val in orjson.loads(options).items():
            data_type = 1
            if type(val) is bool:
                data_type = 2
            elif val.isnumeric():
                data_type = 1

            rapid_options.append({"name": key, "value": val, "dataType": data_type})
        return rapid_options

    async def send(self, variant):
        rapid_options = self.convert_rapid_options(variant)
        data = {
            "applicationName": "RENAISSANCETECH_DEM",
            "instanceName": "RENAISSANCETECH_DEM",
            "serviceUrl": "https://configurator.inforcloudsuite.com/api/v3/ProductConfigurator.svc",
            "partNamespace": "Demo",
            "partNumber": "OfficeTable",
            "profile": "Default",
            "headerId": "config_tester",
            "rapidOptions": rapid_options,
            "detailId": "",
            "sourceHeaderId": "",
            "sourceDetailId": "",
            "pageCaption": "",
            "redirectUrl": "",
        }
        try:
            resp = await self.client.post(
                self.endpoint,
                headers={"Content-Type": "application/json"},
                data=orjson.dumps(data),
            )

            pprint(rapid_options)
            print(resp.status_code)
            pprint(resp.text)

            return resp.status_code, resp.text
        except httpx.ReadTimeout:
            return "TimeoutError", None
