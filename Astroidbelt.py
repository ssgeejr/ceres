import os

class DwarfMoon:
    def __init__(self):
        # Get the path to the .openai directory in the user's home directory
        home_directory = os.path.expanduser('~')
        file_path = os.path.join(home_directory, '.ceres', 'auth.nfo')
        with open(file_path, 'r') as f:
            contents = f.read()
        for line in contents.splitlines():
            key, value = line.split('~')
            if key == 'SERVER':
                self.server = value.strip()
            if key == 'DB':
                self.db = value.strip()
            if key == 'USERNAME':
                self.username = value.strip()
            if key == 'PASSWORD':
                self.password = value.strip()

    def getServer(self):
        return self.server

    def getDB(self):
        return self.db

    def getUsername(self):
        return self.username

    def getPassword(self):
        return self.password
