import json
import urllib.request
import urllib.error


def get_weather_alert(latitude: float, longitude: float):
    # Set up the User-Agent header as required by weather.gov API
    headers = {
        'User-Agent': 'Python Script'
    }

    try:
        # Get the current weather alerts
        alerts_url = f"https://api.weather.gov/alerts?point={latitude},{longitude}"

        req = urllib.request.Request(alerts_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())

        features = data.get('features', [])
        if not features:
            return {
                'statusCode': 204,
                'body': json.dumps('No weather alerts found for this location.')
            }

        props = features[0].get('properties', {})
        record = (
            props.get('sent'),
            props.get('effective'),
            props.get('onset'),
            props.get('expires'),
            props.get('messageType'),
            props.get('severity'),
            props.get('urgency'),
            props.get('event'),
            props.get('sender'),
            props.get('senderName'),
            props.get('headline'),
            props.get('description'),
            props.get('instruction'),
            props.get('response')
        )
        return record

    except urllib.error.HTTPError as http_err:
        return {
            'statusCode': http_err.code,
            'body': json.dumps(f'HTTP error occurred: {http_err.reason}')
        }
    except Exception as exc:
        return {
            'statusCode': 500,
            'body': json.dumps(f'An unexpected error occurred: {str(exc)}')
        }

if __name__ == "__main__":
    print(get_weather_alert(41.8781, -87.6298))