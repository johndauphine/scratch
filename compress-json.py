import gzip
import base64
import pandas as pd

def lambda_handler(event, context):
    df = pd.DataFrame.from_dict([{"name": "John", "age": 30}, {"name": "Jane", "age": 25}])
    
    json_data = df.to_json(orient='records')
   # json_data = {"name":"john"}]
    compressed_data = gzip.compress(json_data.encode('utf-8'))
    encoded_data = base64.b64encode(compressed_data).decode('utf-8')

    return {
        'statusCode': 200,
        'body': encoded_data,
        'isBase64Encoded': True,
        'headers': {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip'
        }
    }

print(lambda_handler(None,None))