from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests

TEST_TOPIC = "Distributed Systems"
FAKE_TOPIC = "Systems"
TEST_MESSAGE = "Deadline is on March 3"
PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 1.0
NUM_NODES_ARRAY = [5]
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3


@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    yield node
    node.clean()

@pytest.fixture
def node():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    yield node
    node.clean()

@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


# Message Queue
def test_create_and_get_multiple_topics_with_duplicates(node):
    topics = []
    for i in range(3):
        topic = TEST_TOPIC + str(i)
        assert(node.create_topic(topic).json() == {"success": True})
        topics.append(topic)
        assert(node.get_topics().json() == {
           "success": True, "topics": topics})
        assert(node.create_topic(topic).json() == {"success": False})
        assert(node.get_topics().json() == {
           "success": True, "topics": topics})
    

def test_put_and_get_wrong_message(node_with_test_topic):
    assert(node_with_test_topic.put_message(
        TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})
    assert(node_with_test_topic.get_message(
        FAKE_TOPIC).json() == {"success": False})
    assert(node_with_test_topic.get_message(
        TEST_TOPIC).json() == {"success": True, "message": TEST_MESSAGE})
    

def test_put2_and_get3_message(node_with_test_topic):
    second_message = TEST_MESSAGE + "2"
    assert(node_with_test_topic.put_message(
        TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})
    assert(node_with_test_topic.put_message(
        TEST_TOPIC, second_message).json() == {"success": True})
    assert(node_with_test_topic.get_message(
        FAKE_TOPIC).json() == {"success": False})
    assert(node_with_test_topic.get_message(
        TEST_TOPIC).json() == {"success": True, "message": TEST_MESSAGE})
    assert(node_with_test_topic.get_message(
        TEST_TOPIC).json() == {"success": True, "message": second_message})
    assert(node_with_test_topic.get_message(
        TEST_TOPIC).json() == {"success": False})
    

# Election
def collect_leaders_in_buckets(leader_each_terms: dict, new_statuses: list):
    for i, status in new_statuses.items():
        assert ("term" in status.keys())
        term = status["term"]
        assert ("role" in status.keys())
        role = status["role"]
        if role == LEADER:
            leader_each_terms[term] = leader_each_terms.get(term, set())
            leader_each_terms[term].add(i)


def assert_leader_uniqueness_each_term(leader_each_terms):
    for leader_set in leader_each_terms.values():
        assert (len(leader_set) <= 1)


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_leader_re_election_after_failure(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader1 != None)
    leader1.clean()
    time.sleep(ELECTION_TIMEOUT * 2)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader2 != None)
    assert (leader2 != leader1)


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_there_duplicate_leader(swarm: Swarm, num_nodes: int):
    leader_each_terms = {}
    statuses = swarm.get_status()
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader1 != None)
    leader1.pause()
    time.sleep(ELECTION_TIMEOUT * 2)
    leader1.resume()
    time.sleep(ELECTION_TIMEOUT * 2)

    collect_leaders_in_buckets(leader_each_terms, statuses)

    assert_leader_uniqueness_each_term(leader_each_terms)

# Replication
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topic_message_shared_loop(swarm: Swarm, num_nodes: int):

    for i in range(num_nodes):
        topic = TEST_TOPIC + str(i)
        message = TEST_MESSAGE + str(i)
        leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

        assert (leader != None)
        assert (leader.create_topic(topic).json() == {"success": True})
        assert (leader.put_message(topic, message).json()
                == {"success": True})
        leader.restart()

    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert(leader1.get_message(FAKE_TOPIC).json()
            == {"success": False})
    
    for i in range(num_nodes):
        topic = TEST_TOPIC + str(i)
        message = TEST_MESSAGE + str(i)
        assert(leader1.get_message(topic).json()
            == {"success": True, "message": message})
