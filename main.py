import base64
import os
import requests
import json

# Задаем адрес Consul
CONSUL_API_URL = "http://localhost:8500/v1/"
CONSUL_API_TOKEN = "e95b599e-166e-7d80-08ad-aee76e7ddf19"

# Читаем файл свойств
with open("example.properties", "r") as f:
    props = f.read()

# Превращаем прочитанные свойства в словарь
props_dict = dict(line.strip().split("=") for line in props.split("\n") if line)

# Формируем список операций для транзакции
txns = []
i = 0
for key, value in props_dict.items():
    # Формируем ключ в Consul в соответствии с заданным форматом
    consul_key = f"{key.replace('.', '/')}"
    # Получаем значение ключа в Consul
    consul_resp = requests.get(
        CONSUL_API_URL + "kv/" + consul_key,
        headers={
            "X-Consul-Token": CONSUL_API_TOKEN
        }
    )
    value_b64 = base64.b64encode(value.encode('utf-8')).decode('utf-8')
    if consul_resp.status_code == 200:
        # Если значение ключа в Consul отличается от значения в файле свойств
        if consul_resp.text != value:
            txns.append({
                "KV": {
                    "Verb": "set",
                    "Key": consul_key,
                    "Value": value_b64
                }
            })
    else:
        txns.append({
            "KV": {
                "Verb": "set",
                "Key": consul_key,
                "Value": value_b64
            }
        })
    i += 1
    # Если мы дошли до максимального количества операций в транзакции
    if i == 64:
        try:
            # Отправляем операции в рамках текущей транзакции в Consul API
            resp = requests.put(CONSUL_API_URL + "txn",
                                json=txns,
                                headers={
                                    "X-Consul-Token": CONSUL_API_TOKEN
                                })
            # Сбрасываем список операций
            txns = []
            # Сбрасываем счетчик
            i = 0
            # Проверяем, что запрос завершился успешно
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error posting transaction to Consul: {e}")
            break

# Если в транзакции есть некоторые добавленные операции, отправляем их в Consul API
if txns:
    try:
        resp = requests.put(CONSUL_API_URL + "txn",
                            json=txns,
                            headers={
                                "X-Consul-Token": CONSUL_API_TOKEN
                            })
        # Проверяем, что запрос завершился успешно
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error posting transaction to Consul: {e}")
