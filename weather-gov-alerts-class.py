import json
import urllib.request
import urllib.error

class WeatherAlertsHandler:
    def process(self,latitude:float, longitude:float):
    

        # Set up the User-Agent header as required by weather.gov API
        headers = {
            'User-Agent': 'Python Script'
        }


        # Get the current weather alerts
        alerts_url = f"https://api.weather.gov/alerts?point={latitude},{longitude}"

        req = urllib.request.Request(alerts_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())


        record =(
            data['features'][0]['properties']['sent'],
            data['features'][0]['properties']['effective'],
            data['features'][0]['properties']['onset'],
            data['features'][0]['properties']['expires'],
            data['features'][0]['properties']['messageType'],
            data['features'][0]['properties']['severity'],
            data['features'][0]['properties']['urgency'],
            data['features'][0]['properties']['event'],
            data['features'][0]['properties']['sender'],
            data['features'][0]['properties']['senderName'],
            data['features'][0]['properties']['headline'],
            data['features'][0]['properties']['description'],
            data['features'][0]['properties']['instruction'],
            data['features'][0]['properties']['response']
        )

        
        return record


        
if __name__ == "__main__":
    print(WeatherAlertsHandler.process(self=None,latitude=29.5822,longitude=-95.7608))