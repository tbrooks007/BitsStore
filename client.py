import threading
import config.config_loader
import hashlib
import models.node

#constraint of exercise
MAX_NUMBER_OF_PAIRS = 10000000

class Client(threading.local):

    def __init__(self):
        super(Client, self).__init__()
        self.__config = config.config_loader.ConfigurationLoader("client", "config").load_config()

        print self.__config

        #this would store an index of nodes (servers) that hold the key/value stores
        #for testing purposes of this exercise it will just hold a collection of dicts
        self.__nodes = self.__load_nodes()

    def put(self, key, value, expiration=0):
        """
            Execute put operation for given key, value pair.
        """

        if self.__nodes:

            #step 1: calculate node hash
            node_index = self.__calculate_node_index(key)

            #step 2: choose server by hash
            node = self.__nodes[node_index]

            print node_index

            self.__execute_put(key, value, node, expiration)

    def get(self, key):
        """
            Execute get operation for given key.
        """
        value = None

        if self.__nodes:
            #step 1: calculate node hash
            node_index = self.__calculate_node_index(key)

            #step 2: choose server by hash
            node = self.__nodes[node_index]

            try:
                #step 3: if these were a real socket servers we'd open the connection and send the request etc
                #but it's not so just add to dictionary
                value = node[key]
            except:
                raise

        return value

    def delete(self, key):
        """
            Execute delete operation on given key.
        """
        if self.__nodes:
            #step 1: calculate node hash
            node_index = self.__calculate_node_index(key)

            #step 2: choose server by hash
            node = self.__nodes[node_index]

            #remove key/value pair
            node.pop(key, None)

    # private helpers

    def __execute_put(self, key, value, node, expiration=0):
        """
            Execute put operation on given node
        """

        #TODO: check if we've reached 10M key/value pairs among all of our nodes

        if expiration > 0:
            node.set_with_expire(key, value, expiration)
        else:
            node[key] = value

        print node

    def __get_node_stats(self, node):
        """
            In "real life" this method would make a socket request to the Node server in question.
            It would make a 'stats' requests and get stats info about the server...this method fakes that call.
        """

        if node:
            stats = node.stats()
            stats['number_of_kv_pairs'] = len(node)
            stats['total_memory_used'] = sys.getsizeof(node)
            return stats
        else:
            raise Exception("Node is 'None'")

    def __calculate_node_index(self, key):
        """
            Calculates a node "index"
        """

        node_index = -1

        #note, keys are made into strings so we can easily calculate the node index for a given key
        if key and self.__nodes:
            digest = hashlib.md5(str(key)).hexdigest().encode('hex')
            node_index = int(digest, 16) % len(self.__nodes)
        else:
            if not self._nodes:
                raise Exception('You must configure server nodes.')

        return node_index

    def __load_nodes(self):

        temp_nodes = []

        for node_config in self.__config.get("nodes"):
            curr_node = models.node.Node(name=node_config['name'])
            temp_nodes.append(curr_node)

        return temp_nodes


if __name__ == "__main__":
    # quick test

    client = Client()
    client.put(1, "yay")
    client.put(10000, "longer value and diff key")
    client.put(10000000, "hey")

    val = client.get(1)

    print val
