import requests, sys, json
from flask import jsonify


def parse_config_json(fp):
    '''
    Get servers config

    Input: fp (file_path, type str): path to config.json file
    Output: a list of tuple (ip, port) denoting the configs of servers
    '''
    with open(fp, 'r') as file:
        config_json = json.load(file)

    servers = [] # list of tuple in format: (ip, port)

    for address in config_json["addresses"]:
        ip, port = address["ip"], str(address["port"])
        if 'http://' not in ip:
            ip = 'http://' + ip
        assert ip, port
        servers.append((ip, port))

    return servers

def connect_server(servers):
    '''
    Connect to the server leader
    Input: a list of configs of servers
    Ouput: True if can connect to a server leader, False otherwise
    '''
    ip, port = servers[-1]
    url = f"{ip}:{port}/status"
    try:
        response = requests.get(url)
    except Exception as e:
        print(e)
    else:
        if response.status_code == 200:
            status = response.json() # {’role’ : str, ’term’ : int}
            term = status['term']
            if status['role'] != 'Leader': # if the role is not Leader, get the port of the Leader sent by Follower
                port = status['role']
            return ip, port, term
    return None, None, None

def put_topic(ip, port, topic):
    '''
    Put a new topic to the server
    Input: ip, port: config of server leader
        topic: topic to be put to the server
    Output: Returns True if the topic was created, False if the topic could not be created (e.g., if the topic name already exists).
    '''
    port_to_connet = port
    while True:
        url = f"{ip}:{port_to_connet}/topic"
        try:
            response = requests.put(url, json={'topic': topic})
        except Exception as e:
            print(e)
        else:
            if response.json().get('leader'):
                port_to_connet = response.json()['leader']
            else:
                return response.json()['success']
    
def get_topic(ip, port):
    '''
    Get all available messages in the topic
    Input: ip, port: config of server leader
    Output: all available topics
    '''
    port_to_connet = port
    while True:
        url = f"{ip}:{port_to_connet}/topic"
        try:
            response = requests.get(url)
        except Exception as e:
            print(e)
        else:
            if response.json().get('leader'):
                port_to_connet = response.json()['leader']
            elif response.json()['success']:
                return response.json()['topics']
            else:
                return "No available topic."
    
def put_message(ip, port, topic, message):
    '''
    Put a message to topic
    Input: ip, port: config of server leader
            topic: topic to put the message in
            message: message to be put in topic in server
    Output: returns True if the message was added and False if the message could not be added (e.g., if the topic does not exist)
    '''
    port_to_connet = port
    while True:
        url = f"{ip}:{port_to_connet}/message"
        print("CLIENT PUTS MESSAGE")
        try:
            response = requests.put(url, json={'topic' : topic, 'message' : message})
            print("PUT A REQUEST")
        except Exception as e:
            print(e)
        else:
            if response.json().get('leader'):
                port_to_connet = response.json()['leader']
            else:
                return response.json()['success']

def get_message(ip, port, topic):
    '''Get the first message stored in topic'''
    port_to_connet = port
    while True:
        url = f"{ip}:{port_to_connet}/message/{topic}"
        try:
            response = requests.get(url)
        except Exception as e:
            print(e)
        else:
            if response.json().get('leader'):
                port_to_connet = response.json()['leader']
            elif response.json()['success']:
                return response.json()['message']
            else:
                return False


if __name__ == "__main__":
    '''
    Command: python client.py put <topic> <message>
            python client.py get <topic>
    '''
    # Get the configs of all servers
    servers = parse_config_json("config.json")

    if sys.argv[1].lower() == "put":
        if len(sys.argv) == 3:
            '''Put topic'''
            ip, port, term = connect_server(servers)
            if ip is None:
                print("No available server.")
                sys.exit(0)
            topic = sys.argv[2]
            if put_topic(ip, port, topic):
                print("Topic has been created.")
            else:
                print("Topic cannot be created.")

        elif len(sys.argv) == 4:
            '''Put message in topic'''
            ip, port, term = connect_server(servers)
            if ip is None:
                print("No available server.")
                sys.exit(0)
            topic = sys.argv[2]
            message = sys.argv[3]
            if put_message(ip, port, topic, message):
                print("Message has been put in the topic.")
            else:
                print("Message cannot be put in the topic.")

        else:
            print("Command: python client.py put <topic> or python client.py put <topic> <message>")

    elif sys.argv[1].lower() == "get":
        if len(sys.argv) == 3:
            '''Get topic'''
            ip, port, term = connect_server(servers)
            if ip is None:
                print("No available server.")
                sys.exit(0)
            print(get_topic(ip, port))

        elif len(sys.argv) == 4:
            '''Get message from topic'''
            ip, port, term = connect_server(servers)
            if ip is None:
                print("No available server.")
                sys.exit(0)
            topic = sys.argv[3]
            message = get_message(ip, port, topic)
            if isinstance(message, str):
                print(message)
            else:
                print("Topic does not exist or there are no messages in the topic that haven't been already consumed")

        else:
            print("Command: python client.py get topic or python client.py get message <topic>")

    else:
        print("Invalid command")