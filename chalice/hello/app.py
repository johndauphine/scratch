from chalice import Chalice

import pandas
import requests
import marshmallow

app = Chalice(app_name='hello')


@app.route('/')
def index():
    return {'message': 'Hello world'}


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
@app.route('/hello/{name}')
def hello_name(name):
    # '/hello/james' -> {"hello": "james"}
    return {'message': f'Hello {name}'}
#
@app.route('/users', methods=['POST'])
def create_user():
    # This is the JSON body the user sent in their POST request.
    user_as_json = app.current_request.json_body
    # We'll echo the json body back to the user in a 'user' key.
    return {'user': user_as_json}
#
# See the README documentation for more examples.
#
