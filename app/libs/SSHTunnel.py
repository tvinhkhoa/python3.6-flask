import sshtunnel
import paramiko

class SSHTunnel:

    server = None
    is_alive = False
    def tunnel(self):
        return

    def forwarder(self, host={"host":"localhost","port":22}, ssh_username="", ssh_password="", remote={"bind_address":"","bind_port":""}):
        self.host = host['host']
        self.port = host['port']
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.bind_address = remote['bind_address']
        self.bind_port = remote['bind_port']

        try:
            sshtunnel.SSH_TIMEOUT = 5.0
            sshtunnel.TUNNEL_TIMEOUT = 5.0
            self.server = sshtunnel.SSHTunnelForwarder(
                (self.host, int(self.port)),
                ssh_username = self.ssh_username,
                ssh_password = self.ssh_password,
                remote_bind_address = (self.bind_address, int(self.bind_port)),
                # debug_level='TRACE',
            )
            self.server.start()
            self.is_alive = True
        except Exception as e:
            print ("Error connecting to SSH", e)
        return

    def start(self):
        if self.is_alive is False:
            self.server.start()

    def get_local_bind_port(self):
        return self.server.local_bind_port

    def stop(self):
        self.server.stop()