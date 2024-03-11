from flask import Flask, request, jsonify
import sys
import json
from threading import Thread
import requests
from timer import ResettableTimer


STATE = ['Leader', 'Follower', 'Candidate']


def get_port(path_to_config, index=None):
    """
    Function to load the config file and get the PORT and IP 
    in that particular index
    Output:
        Tuple containing IP and PORT
    """
    with open(path_to_config, 'r') as data:
        config = json.load(data)
        if index is not None:
            instance =  config['addresses'][index]
            return instance['ip'], instance['port']
        else:
            return [c['port'] for c in config['addresses']]


class Node:     
    def __init__(self, ip, port, path_to_config,
                 timer_interval_lb=150, timer_interval_up=300,
                 heartbeat_interval_lb=40, heartbeat_interval_up=41):
        self.app = Flask(__name__)
        self.ip = ip
        self.port = port
        self.term = 0
        self.message_queue = dict()

        # command rule inside self.log is denoted in self.decode_command()
        self.log = []

        self.swarm = get_port(path_to_config)
        self.swarm.remove(self.port)

        # if there is a leader or candidate in the swarm, this node is a follower
        follower, leader =  self.leader_or_candidate_exist()
        if follower:
            self.state = STATE[1]
            self.leader = leader # note that if the leader returned by self.leader_or_candidate_exist() can be None, which means that a leader hasn't been chosen

        # if there is no other server in the swarm, this node is the leader
        # if port is the smallest port, this node is also the leader
        elif len(self.swarm) == 0 or self.port < min(self.swarm):
            self.state = STATE[0]
            self.leader = self.port
        else:
            self.state = STATE[1]
            self.leader = min(self.swarm)

        self.timer = ResettableTimer(self.election, interval_lb=timer_interval_lb, interval_ub=timer_interval_up)
        self.heartbeat_timer = ResettableTimer(self.send_heartbeat, interval_lb=heartbeat_interval_lb, interval_ub=heartbeat_interval_up)

        # if node is the leader, do not run the timer
        if self.state != STATE[0]:
            self.timer.run()
        else:
            self.heartbeat_timer.run()

        self.election_responses = []
        self.log_responses = []
        
        self.setup()
    
    def leader_or_candidate_exist(self):
        '''
        Check if there is a Leader or Candidate already in the swarm
        '''
        for port in self.swarm:
            url = f"{ip}:{port}/status"
            try:
                response = requests.get(url)
            except Exception as e:
                pass
            else:
                if response.status_code == 200:
                    status = response.json() # {’role’ : str, ’term’ : int}
                    if status['role'] == STATE[0]:
                        return True, port
                    elif status['role'] == STATE[2]:
                        return True, None
        return False, None
        
    def election(self):
        # only if itself is not a leader
        if self.state == STATE[0]:
            return
        # print("Starting election ...")
        self.election_responses = []
        self.state = STATE[2]
        self.term += 1

        election_threads = []

        for port in self.swarm:
            thr = Thread(target=self.send_election, args=(port,))
            election_threads.append(thr)
            thr.start()

        for thr in election_threads:
            thr.join()

        # if received at least half of votes, become leader
        if len([i for i in self.election_responses if i]) + 1 > len(self.swarm) / 2:
            self.timer.pause()
            self.state = STATE[0]
            self.leader = self.port
            self.heartbeat_timer.run()
            
            # send notifications to other nodes
            noti_threads = []
            for port in self.swarm:
                thr = Thread(target=self.send_message, args=(port,), kwargs={'send_leader': True})
                thr.start()
                noti_threads.append(thr)

            for thr in noti_threads:
                thr.join()

            # execute all the pending command
            for index, log in enumerate(self.log):
                if log[1] == False:
                    self.execute_command_from_index(index)
                    break

            # ensure that other followers have the same log
            log = {'command_index': len(self.log)}
            for port in self.swarm:
                thread = Thread(target=self.ensure_consensus, args=(port, log, True))
                thread.start()
                thread.join()

        # if not received at least half of votes, return back to the follower state        
        else:
            self.state = STATE[1]

    def send_heartbeat(self):
        for port in self.swarm:
            self.send_message(port, send_heartbeat=True)
        self.heartbeat_timer.reset()

    def send_message(self, port, send_leader=False, send_heartbeat=False, log=False, execute=False):
        url = f"{ip}:{port}/internal-message" # change to internal-message because /message is for client-server interaction
        try:
            if send_leader:
                requests.put(url, json={'leader' : self.leader, 'term': self.term})
            elif send_heartbeat:
                requests.put(url, json={'heartbeat' : True, 'term': self.term, 'port': self.port})
            elif log:
                res = requests.put(url, json={'log' : log, 'term': self.term, 'log_from_leader': self.port})
                if res.json().get('Confirm'):
                    self.log_responses.append(res.json()['port'])
            elif execute:
                requests.put(url, json={'execute' : execute, 'term': self.term})
        except requests.exceptions.ConnectionError:
            pass
        else:
            if log:
                # If there is a mismatch either in IndexMessage or PrevMessage, the leader will spawn a new thread to take care of that
                if not res.json()['Confirm']:
                    thread = Thread(target=self.ensure_consensus, args=(port, log, False))
                    thread.start()
                    thread.join()

    def ensure_consensus(self, port, log, back_log=False):
        '''
        Ensure the consensus in other nodes' log
        '''
        # Lookup until a match between leader and follower is found
        command_index = log['command_index'] - 1 # start at the index that does not match
        while command_index >= 0:
            # decrease the command_index until the message is the same as the leader's message
            command = self.log[command_index][0]
            prev_command = self.log[command_index - 1][0] if command_index > 0 else None
            res = requests.put(
                f"{self.ip}:{port}/internal-message",
                json={
                    'trace_log': {
                        'term': self.term,
                        'command': command,
                        'command_index': command_index,
                        'prev_command': prev_command
                    }
                }
            )
            # if a match is found, break the loop
            if res.json().get('Confirm'):
                break
            # if the follower's log is empty, break the loop
            if res.json().get('log_empty'):
                # change the command index to 0
                command_index = 0
                break
            # break the loop if command_index is less than 0
            command_index -= 1

        # command_index is either -1 or the index of the last command that matches the leader's command
        # if command_index is -1, command_index becomes 0
        if command_index < 0:
            command_index = 0

        # if back_log is True, this is only when the a new leader is appointed
        if back_log:
            for i in range(command_index, len(self.log)):
                command = self.log[i][0]
                requests.put(
                    f"{self.ip}:{port}/internal-message",
                    json={
                        'execute_from_index': {
                            'term': self.term,
                        }
                    }
                )
        
        # else, feed information forward
        else:
            for i in range(command_index, len(self.log)):
                command = self.log[i][0]
                requests.put(
                    f"{self.ip}:{port}/internal-message",
                    json={
                        'replace_log': {
                            'term': self.term,
                            'command': command,
                            'command_index': i,
                        }
                    }
                )

    def send_election(self, port):
        '''Send vote request to other nodes in the swarm.'''
        url = f"{ip}:{port}/election"
        try:
            res = requests.put(url, json={'port': self.port, 'term': self.term, 'log_num': len(self.log)}).json()
            self.election_responses.append(res['Confirm'])
        except requests.exceptions.ConnectionError:
            pass
    
    def send_log(self, command):
        '''
        send_log to all the followers
        '''
        log_threads = []
        for port in self.swarm:
            command_index = len(self.log) - 1
            thr = Thread(
                target=self.send_message,
                args=(port,),
                kwargs={
                    'log': {
                        'term': self.term,
                        'command_index': command_index,
                        'command': command,
                        'prev_command': self.log[command_index - 1] if command_index > 0 else None
                    }
                }
            )
            thr.start()
            log_threads.append(thr)

        for log_thread in log_threads:
            # waiting for all the responses from the followers
            log_thread.join()

        return True
        
    def execute_command(self, index):
        '''
        Decode:
        # add topic: put topic <topic>
        # get topic: get topic
        # add message to topic: put message <topic> <message>
        # get message from topic: get message <topic>
        '''
        # if command is already executed, return
        if self.log[index][1]:
            return
        
        command = self.log[index][0]

        if command.startswith('put!@#topic'):
            topic = command.split('!@#')[-1]
            self.message_queue[topic] = list()
            self.log[index] = (command, True)

        elif command.startswith('get!@#topic'):
            self.log[index] = (command, True)
            return list(self.message_queue.keys())
        
        elif command.startswith('put!@#message'):
            topic, message = command.split('!@#')[-2:]
            self.message_queue[topic].append(message)
            self.log[index] = (command, True)

        elif command.startswith('get!@#message'):
            topic = command.split('!@#')[-1]
            self.log[index] = (command, True)
            return self.message_queue[topic].pop(0)
        
    def execute_command_from_index(self, index):
        '''
        Execute the command from the index
        '''
        for i in range(index, len(self.log)):
            self.execute_command(i)

    def execute_command_in_followers(self):
        execute_threads = []
        
        # send the execute command to all followers in self.log_responses
        for port in self.log_responses:
            thr = Thread(
                target=self.send_message,
                args=(port,),
                kwargs={
                    'execute': {
                        'term': self.term,
                    }
                }
            )
            execute_threads.append(thr)
            thr.start()
        
        # wait for all threads to complete
        for thr in execute_threads:
            thr.join()
        
        # empty self.log_repsonses
        self.log_responses = []
    
    def setup(self):
        @self.app.route('/election', methods=['PUT'])
        def respond_election():
            data = request.get_json()
            port, term, log_num = data['port'], data['term'], data['log_num']
            
            if self.term < term and len(self.log) <= log_num:
                # update the term to the maximum term
                self.term = data.get('term')

                # if confirm the leader, reset the timer
                self.timer.reset()

                return jsonify({'Confirm': True}), 200
            
            return jsonify({'Confirm': False}), 404

        @self.app.route('/internal-message', methods=['PUT']) # change to internal-message because /message is for client-server interaction
        def respond_message():
            # reset timer
            self.timer.reset()

            data = request.get_json()

            term = -1

            # update the self.term with the maximum term
            if 'term' in data:
                term = data.get('term')
            elif 'log' in data:
                term = data.get('log').get('term')
            elif 'replace_log' in data:
                term = data.get('replace_log').get('term')
            elif 'trace_log' in data:
                term = data.get('trace_log').get('term')
            elif 'execute_from_index' in data:
                term = data.get('execute_from_index').get('term')
            elif 'heartbeat' in data:
                term = data.get('term')
            elif 'leader' in data:
                term = data.get('term')

            self.term = max(self.term, term)

            if 'leader' in data:
                if self.state == STATE[0]:
                    self.heartbeat_timer.pause()
                self.state = STATE[1]
                self.leader = data.get('leader')
                return jsonify({'Confirm': True}), 200
            
            elif 'heartbeat' in data:
                if self.leader is None:
                    self.leader = data.get('port')
                if self.state == STATE[2]:
                    self.state = STATE[1]
                    self.leader = data.get('port')
                return jsonify({}), 200
            
            elif 'execute' in data:
                # execute the command in the follower
                self.execute_command(len(self.log) - 1)
                return jsonify({'Confirm': True}), 200
            
            elif 'log' in data:
                # if node is currently Candidate, revert back to Follower
                if self.state == STATE[2]:
                    self.state = STATE[1]
                    self.leader = data.get('log_from_leader')

                # if the follower has no log, return False
                if len(self.log) == 0:
                    return jsonify({'Confirm': False, 'port': self.port}), 404
                
                command_index = data['log']['command_index']
                prev_command = data['log']['prev_command']

                # if the index message is equal to the follower's next index message
                if command_index == len(self.log):
                    # if the prev message is equal to the follower's last message
                    if prev_command is None or prev_command == self.log[-1]:

                        # add the command to the follower's log and mark the command has not been executed
                        self.log.append((data['log']['command'], False))

                        return jsonify({'Confirm': True, 'port': self.port}), 200
                    else:
                        # the prev message of the leader is not the same as the last message of the follower
                        return jsonify({'Confirm': False, 'port': self.port}), 404
                else:
                    # the index message of the leader is not the same as the next index message of the follower
                    return jsonify({'Confirm': False, 'port': self.port}), 404
            
            elif 'replace_log' in data:
                command = data['replace_log']['command']
                command_index = data['replace_log']['command_index']

                # if command_index is more than the length of the follower's log, append the command to the follower's log
                if command_index >= len(self.log):
                    self.log.append((command, False))
                else:
                    self.log[command_index] = (command, False)

                self.execute_command(command_index)

                return jsonify({'Confirm': True}), 200
            
            elif 'trace_log' in data:
                # trace the log of the follower to the first position where the leader's log and the follower's log are the same
                command = data['trace_log']['command']
                command_index = data['trace_log']['command_index']
                prev_command = data['trace_log']['prev_command']

                # if follower's log is empty, return False and notify leader that the log is empty
                if len(self.log) == 0:
                    return jsonify({'Confirm': False, 'log_empty': True}), 404
                
                # if the follower's command at command_index matches the leader's command, return True
                elif self.log[command_index][0] == command:
                    # might be unnecessary
                    self.log = self.log[:command_index + 1]
                    return jsonify({'Confirm': True}), 200
                
                # else, the follower's command at command_index does not match the leader's command, pop it out of self.log and self.execute_log_changes
                else:
                    self.log = self.log[:command_index]
                    return jsonify({'Confirm': False}), 404
                
            elif 'execute_from_index' in data:
                # execute the command from the index
                self.execute_command_from_index(0)
                return jsonify({'Confirm': True}), 200

        # Interact with client
        @self.app.route('/topic', methods=['PUT'])
        def add_topic():
            """
            Function to add the potential topic to the database
            Output:
                JSON object with status of the operation
            """
            if self.state != STATE[0]:
                return jsonify({'leader': self.leader}), 404
            success = False
            data = request.get_json()
            topic = data.get('topic')
            if topic and topic not in self.message_queue:
                command = f"put!@#topic!@#{topic}"

                # add put topic <topic> in self.log and mark the command has not been executed
                self.log.append((command, False))
                
                # after every followers have the same log, execute those logs in the followers
                if self.send_log(command):
                    self.execute_command_in_followers()
                
                # execute the command in the leader
                self.execute_command(len(self.log) - 1)

                success = True
            return jsonify({'success': success}), 201 if success else 404

        # Interact with client
        @self.app.route('/topic', methods=['GET'])
        def get_topics():
            """
            Function to fetch all the available topics in the self.message_queue
            Output:
                JSON object with status and the retrieved list of topics
            """
            if self.state != STATE[0]:
                return jsonify({'leader': self.leader}), 404
            # add get topic <topic> in self.log
            command = "get!@#topic"

            # add put topic <topic> in self.log and mark the command has not been executed
            self.log.append((command, False))
            
            # send log to all followers
            self.send_log(command)
            
            # execute the command in the leader
            topics = self.execute_command(len(self.log) - 1)

            return jsonify({'success': True, 'topics': topics}), 200

        # Interact with client
        @self.app.route('/message', methods=['PUT'])
        def add_message():
            """
            Function to add a message into a topic
            Output:
                JSON object with status of the operation
            """
            if self.state != STATE[0]:
                return jsonify({'leader': self.leader}), 404
            
            topic = request.json['topic']
            message = request.json['message']
            if topic in self.message_queue:
                command = f"put!@#message!@#{topic}!@#{message}"
                
                # add put topic <topic> in self.log and mark the command has not been executed
                self.log.append((command, False))
                
                # after every followers have the same log, execute those logs in the followers
                if self.send_log(command):
                    self.execute_command_in_followers()
                
                # execute the command in the leader
                self.execute_command(len(self.log) - 1)

                return jsonify({'success': True}), 201
            return jsonify({'success': False}), 404

        # Interact with client
        @self.app.route('/message/<topic>', methods=['GET'])
        def consume_message(topic):
            """
            Function to retrieve the first available message in the topic
            Input:
                topic (str): the keyword used to fetch that particular message
            Output:
                JSON object with status and the retrieved message
            """
            if self.state != STATE[0]:
                return jsonify({'leader': self.leader}), 404
            
            # if topic is not in the message_queue or the message_queue is empty
            if (not self.message_queue.get(topic)) or (len(self.message_queue[topic]) == 0):
                return jsonify({'success': False}), 200
            
            command = f"get!@#message!@#{topic}"

            # add put topic <topic> in self.log and mark the command has not been executed
            self.log.append((command, False))
            
            # after every followers have the same log, execute those logs in the followers
            if self.send_log(command):
                self.execute_command_in_followers()
            
            # execute the command in the leader
            message = self.execute_command(len(self.log) - 1)

            return jsonify({'success': True, 'message': message}), 200

        # Interact with client
        @self.app.route('/status', methods=['GET'])
        def get_status():
            """
            Function to get the status of the node
            Output:
                JSON object with the role and the term of the node if it is a leader
                else it send the port of the leader and the term of the node
            """
            return (jsonify({'role': self.state, 'term': self.term}), 200) if self.state == STATE[0]\
                    else (jsonify({'role': self.leader, 'term': self.term}), 200)
        
    def run(self):
        self.app.run(host=self.ip.split('//')[1], port=self.port)

 
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Try: python src/node.py <path_to_config> <index>")
        sys.exit(1)
    path_to_config = sys.argv[1]
    index = int(sys.argv[2])
    ip, port = get_port(path_to_config, index)
    ip = 'http://' + ip if 'http://' not in ip else ip
    node = Node(ip, port, path_to_config)
    node.run()