class PingIPFSNodeFailedException(Exception):
    def __init__(self, status_code):
        self.message = f"Failed to ping IPFS node with status code {status_code}"
        super().__init__(self.message)
