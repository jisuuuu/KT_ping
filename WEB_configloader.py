# Load WEB config
# author : Choijh

def get_web_configs(file_path):
    config = {}

    with open(file_path, encoding='utf-8') as f:
        for line in f.readlines():
            if line[0] == '#':
                continue
            if "=" in line:
                conf = line.split("=")
                config[conf[0]] = conf[1].strip()

        return config
