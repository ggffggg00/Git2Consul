import base64
import glob
import os
import consul


CONSUL_URL = "http://localhost:8500/"
CONSUL_API_TOKEN = "e95b599e-166e-7d80-08ad-aee76e7ddf19"
SCAN_EXTENSIONS = ('.yml', '.yaml', '.properties')


with open(".g2c", "r") as f:
    settings_file = f.read()
settings = dict(line.strip().split("=") for line in settings_file.split("\n") if line)

scan_path = settings["scan_dir"] or './'
files = []
for ext in SCAN_EXTENSIONS:
    files.extend(glob.glob(f"{scan_path}/**/*{ext}", recursive=True))


files_with_relative_path = [os.path.relpath(file, scan_path).replace("\\", "/") for file in files]


def parse_key_segments(*args):
    segments = list()
    for el in args:
        if type(el) is list:
            segments.extend(el)
        if type(el) is str:
            segments.extend(str(el).replace("\\", "/").replace(".", "/").split("/"))
    return segments


for file_name in files_with_relative_path:

    txns = []

    with open(settings["scan_dir"] + file_name, "r") as f:
        props = f.read()

    file_props_dict = dict(line.strip().split("=") for line in props.split("\n") if line)

    file_consul_k_segments = parse_key_segments(settings["root_k"], f"{file_name.replace('.properties', '')}")
    curr_kv_dict = consul.read_kv_recursively(CONSUL_URL, CONSUL_API_TOKEN, "/".join(file_consul_k_segments))

    if "false" == settings["include_file_name"]:
        file_consul_k_segments.pop()

    file_props_dict = dict(("/".join(parse_key_segments(file_consul_k_segments, k)),
                                  v) for k, v in file_props_dict.items())
    file_props_keys_set = set(file_props_dict.keys())
    consul_props_keys_set = set(curr_kv_dict.keys())

    keys_to_remove = consul_props_keys_set - file_props_keys_set

    keys_to_iterate = {item: "set" for item in file_props_keys_set}
    keys_to_iterate.update({item: "delete" for item in keys_to_remove})

    for key, operation_type in keys_to_iterate.items():
        tx_operation = {
            "Verb": operation_type,
            "Key": key
        }

        if operation_type == "set":
            value_b64 = base64.b64encode(file_props_dict[key].encode('utf-8')).decode('utf-8')
            if key not in curr_kv_dict or curr_kv_dict[key] != value_b64:
                tx_operation["Value"] = value_b64
                txns.append({
                    "KV": tx_operation
                })
        elif operation_type == "delete":
            txns.append({
                "KV": tx_operation
            })

    if txns:
        consul.write_txn(CONSUL_URL, CONSUL_API_TOKEN, txns)
