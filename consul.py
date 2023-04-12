import requests
import json


def _get_http_headers(acl_token: ""):
    return {
        "X-Consul-Token": acl_token
    }


def read_kv_recursively(consul_url, acl_token, root_k: ""):
    try:
        consul_resp = requests.get(consul_url + "v1/kv/" + root_k + "?recurse=true",
                                   headers={
                                       "X-Consul-Token": acl_token
                                   })
        if consul_resp.status_code == 404:
            return dict()
        return {dct["Key"]: dct["Value"] for dct in json.loads(consul_resp.text)}
    except requests.exceptions.HTTPError as e:
        print(f"Error while fetching KV values from Consul: {e}")
    pass


def write_txn(consul_url, acl_token, data):
    try:
        resp = requests.put(consul_url + "v1/txn",
                            json=data,
                            headers=_get_http_headers(acl_token))
        # Проверяем, что запрос завершился успешно
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error posting transaction to Consul: {e}")
