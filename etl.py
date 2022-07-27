import json
import requests
import pandas as pd
from datetime import datetime
from prefect import task, Flow, Parameter


@task
def extract(url: str) -> dict:
    res = requests.get(url)
    if not res:
        raise Exception('No data fetched!')
    return json.loads(res.content)


@task
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for user in data:
        transformed.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })
    return pd.DataFrame(transformed)


@task
def load(data: pd.DataFrame):
    res = data.to_json(orient="records")
    return res


def prefect_flow():
    with Flow(name='etl_pipeline') as flow:
        param_url = Parameter(name='p_url', required=True)

        users = extract(url=param_url)
        df_users = transform(users)
        res = load(data=df_users)

    return flow



if __name__ == '__main__':
    flow = prefect_flow()
    flow.run(parameters={
        'p_url': 'https://jsonplaceholder.typicode.com/users'
    })