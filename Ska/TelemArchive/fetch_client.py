import socket
from signal import signal, SIGALRM, alarm
import pickle

# user-accessible port
HOST = 'baffin'
PORT = 18039

# Unlikely sequence of characters to terminate conversation
TERMINATOR = "\_$|)<~};!)}]+/)()]\;}&|&*\\%_$^^;;-+=:_;\\<|\'-_/]*?`-"

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError

def send(action, host=HOST, port=PORT):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.connect((host, port))
    except socket.error:
        return [dict(client_error='Socket connection error')]

    try:
        alarm(5)

        msg = pickle.dumps(action) + TERMINATOR
        while msg:
            n = server.send(msg)
            msg = msg[n:]

        # read the response
        msg = ''
        while TERMINATOR not in msg:
            msg += server.recv(1024)

        response = pickle.loads(msg[:-len(TERMINATOR)])

        alarm(0)
    except TimeoutError, e:
        response = [dict(client_error='TimeoutError')]

    server.close()
    return response

