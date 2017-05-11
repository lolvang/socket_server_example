import socket

RECV_SIZE = 8192


def read_bytes(sock, bytes_to_read):
    buffer = ""
    while bytes_to_read > len(buffer):
        received = sock.recv(RECV_SIZE)
        if not received:
            raise Exception("Connection closed")
        buffer += received

    return buffer


def read_response(sock):
    buffer = ""
    while True:
        received = sock.recv(RECV_SIZE)
        if not received:
            raise Exception("Connection closed")
        buffer += received
        if "\n" in buffer:
            break

    header, payload = buffer.split("\n", 1)
    status, size = header.split("\t")
    payload += read_bytes(sock, int(size) - len(payload))
    return status, payload


def send_command(sock, command, key, payload=""):
    request = command + "\t" + key + "\t" + str(len(payload)) + "\n" + payload
    #print "Request: " + repr(request)
    sock.sendall(request)
    return read_response(sock)


def get_key(sock, key):
    return send_command(sock, "get", key)


def set_key(sock, key, data):
    return send_command(sock, "set", key, data)


def delete_key(sock, key):
    return send_command(sock, "delete", key)


def get_num_connections(sock):
    return send_command(sock, "stats", "num_connections")


def get_db_size(sock):
    return send_command(sock, "stats", "db_size")


def get_num_keys(sock):
    return send_command(sock, "stats", "num_keys")

def test(nr,key_size,val_size,do_get, do_del):
    import time
    from random import randint as rnd
    print "generating data"
    lst = [("".join([str(rnd(0,9)) for n in xrange(key_size)]), "".join([str(rnd(0,9)) for n in xrange(val_size)]) ) for n in xrange(nr)]
    print "starting test"
    start = time.time()
    sock = socket.create_connection(("", 3434))
    count = 0    
    for key,val in lst:
        count += 1        
        res = set_key(sock,key,val)
        if res[0] != "ok":
            print "insert failed: " + res[0]
        if count % 10000 == 0:
            print "set "+ str(count)
    if do_get:
        count = 0
        for key,val in lst:
            count += 1     
            r = get_key(sock,key) 
            if count % 10000 == 0:
                print "get "+ str(count)
            if r[0] == "db_full":
                print "db_full"      
            elif r[0] == "ok" and r[1] != val:
                print "%s key %s got %s shoudl be %s" % (r[0],key, r[1], val)
    if do_del:
        count = 0
        for key,val in lst:
            count += 1
            delete_key(sock,key)
            if count % 10000 == 0:
                print "del "+ str(count)
    end = time.time()
    print "done in %s" % (end-start)




if __name__ == "__main__":
    test(340000,32,32, True, True)
    
    
    
    #print set_key(sock, "foo", "bar")
    #print get_key(sock, "foo")
    #print get_num_connections(sock)
    #print get_num_keys(sock)