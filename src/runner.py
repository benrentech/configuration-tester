from pprint import pprint
import httpx
import orjson


class Runner:
    """
    Runner class handles the transformation of configuration data and sending it
    to a remote endpoint using an asynchronous HTTP client.
    """

    def __init__(self, endpoint):
        """
        Initializes the Runner with a target endpoint and an HTTP/2-capable async client.

        Args:
            endpoint (str): The URL to which configuration data will be posted.
        """
        self.endpoint = endpoint
        # Use HTTP/2 and a short timeout for fast request turnaround
        self.client = httpx.AsyncClient(http2=True, timeout=1.0)

    def convert_rapid_options(self, options):
        """
        Converts a JSON-encoded string of configuration options into the format expected by the API.

        Args:
            options (str): JSON string representing a dictionary of options.

        Returns:
            list[dict]: A list of dictionaries, each representing an option with a name, value, and data type.

        Detailed behavior:
            - The input string is deserialized using `orjson.loads`.
            - Each key-value pair is processed:
                - `dataType` is set based on value type:
                    - `1` for strings or numerics
                    - `2` for booleans
                - Booleans are explicitly checked with `type(val) is bool`.
                - Numerics are identified using `val.isnumeric()` for strings.
            - Output format:
                ```python
                {
                    "name": <option name>,
                    "value": <option value>,
                    "dataType": <1 for string/number, 2 for boolean>
                }
                ```
        """
        rapid_options = []
        for key, val in orjson.loads(options).items():
            data_type = 1  # Default to string/number
            if type(val) is bool:
                data_type = 2
            elif isinstance(val, str) and val.isnumeric():
                data_type = 1

            rapid_options.append({"name": key, "value": val, "dataType": data_type})

        return rapid_options

    async def send(self, variant):
        """
        Sends a configuration variant to the endpoint using an HTTP POST request.

        Args:
            variant (str): A JSON string containing configuration options.

        Returns:
            tuple: (HTTP status code or error name, response text or None)

        Detailed behavior:
            - Converts the input `variant` to a list of `rapidOptions` using `convert_rapid_options`.
            - Constructs a POST payload with fixed metadata (like `partNumber`, `profile`, etc.).
            - Sends the request to the configured endpoint using `httpx.AsyncClient`.
            - If successful:
                - Returns `(status_code, response_text)`
            - If the request times out (`httpx.ReadTimeout`):
                - Returns `("TimeoutError", None)`

        Notes:
            - The request uses HTTP/2 and has a 1-second timeout.
            - The `Content-Type` is explicitly set to `application/json`.
            - Data is serialized using `orjson.dumps` for speed and efficiency.
        """
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

            # Uncomment the following for debugging:
            # pprint(rapid_options)
            # print(resp.status_code)
            # pprint(resp.text)

            return resp.status_code, resp.text
        except httpx.ReadTimeout:
            return "TimeoutError", None
