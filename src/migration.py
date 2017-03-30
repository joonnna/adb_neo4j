from neo4j.v1 import GraphDatabase, basic_auth
import time
import os
import yaml


def add_user(driver, email, name, status, ip, friends_list):
    ''' Add User node to graph 
        args:
            string email      = E-mail of user
            string name       = Name of user
            string status     = 'Online' or 'Offline'
            string ip         = IP-address of user
            list friends_list = List of friends (e-mail)
    '''

    insert_query = "CREATE (u:User {email: '%s', name: '%s', status: '%s', ip: '%s'})" % (email, name, status, ip)
    
    with driver.session() as session:
        results = session.run(insert_query)
        session.sync()
        
        for friend in friends_list:
            friends_insert = "MATCH (a:User {email: '%s'}), (b:User {email: '%s'}) MERGE (a)-[:FRIEND_OF]->(b)" % (email, friend)
            results = session.run(friends_insert)
            session.sync()



def add_message(driver, created_by, created_at, content):
    ''' Add Message node to graph 
        args:
            string created_by = E-mail of user
            string created_at = Time stamp (dd-mm-yy-hh-mm-ss)
            string content    = Content of message
    '''

    insert_message = "MATCH (u:User {email: '%s'}) CREATE (m:Message {created_at: '%s', content: '%s'})-[:POSTED_BY]->(u)" % (created_by, created_at, content)

    with driver.session() as session:
        res = session.run(insert_message)
        session.sync()



def add_file(driver, email, path, ftype):
    ''' Add single File node to graph 
        args:
            string email = E-mail of owner
            string path  = Absolute path of file
            string ftype = 'Online' or 'Offline'
    '''

    path_list = path.split('/')
    filename = path_list[-1]
    path_list.pop()
    path_list.pop(0)

    # Get file extension
    ext = filename.split(".")
    ext = "." + ext[-1]

    res_path = os.path.dirname(path)
    
    if len(path_list) < 1:
        query = "MATCH (u:User {email: '%s'}) MERGE (f:File {name: '%s', path: '%s', type: '%s', ext: '%s'})-[:SHARED_BY]->(u)" % (email, filename, path, ftype, ext)
    else:
        cur_folder = path_list[0]
        count = 1
        count2 = 2
        file1 = "file1"
        file2 = "file2"

        query = "MATCH (u:User {email: '%s'})-[:SHARED_BY]-(%s) " % (email, file1)

        for item in path_list[1:]:
            query += "MATCH (%s)-[:CONTAINS]->(%s) " % (file1, file2)
            count += 1
            count2 += 1
            file1 = "file%s" % (str(count))
            file2 = "file%s" % (str(count2))
        query += "WHERE %s.path = '%s' " % (file1, res_path)
        query += "MERGE (f:File {name: '%s', path: '%s', type: '%s', ext: '%s'})<-[:CONTAINS]-(%s)" % (filename, path, ftype, ext, file1)

    with driver.session() as session:
        res = session.run(query)
        session.sync()




def add_folder(driver, email, name, path):
    ''' Add Folder node to graph
        args:
            string email = E-mail of owner
            string name  = Folder name
            string path  = 'Online' or 'Offline'
    '''

    path_list = path.split('/')
    path_list.pop()
    path_list.pop(0)
    res_path = os.path.dirname(path)

    if len(path_list) < 1:
        query = "MATCH (u:User {email: '%s'}) MERGE (f:Folder {name: '%s', path: '%s'})-[:SHARED_BY]->(u)" % (email, name, path)
    else:
        cur_folder = path_list[0]
        count = 1
        count2 = 2
        file1 = "file1"
        file2 = "file2"

        query = "MATCH (u:User {email: '%s'})-[:SHARED_BY]-(%s) " % (email, file1)

        for item in path_list[1:]:
            query += "MATCH (%s)-[:CONTAINS]->(%s) " % (file1, file2)
            count += 1
            count2 += 1
            file1 = "file%s" % (str(count))
            file2 = "file%s" % (str(count2))
        query += "WHERE %s.path = '%s' " % (file1, res_path)
        query += "MERGE (f:Folder {name: '%s', path: '%s'})<-[:CONTAINS]-(%s)" % (name, path, file1)

    with driver.session() as session:
        res = session.run(query)
        session.sync()



def add_full_path(driver, email, path, size, filetypes):
    ''' Adds File node and all its parent folders.
        args:
            string email   = E-mail of owner
            string path    = Full path of file
            dict size      = Dictionary of all file sizes
            dict filetypes = Dictionary for defining file type (Picture, Video, etc.)
    '''

    folder_path = os.path.dirname(path)
    folder_list = folder_path.split('/')
    folder_list.pop(0)
    filename = path.split('/')[-1]

    # Get file extension
    ext = filename.split(".")
    ext = "." + ext[-1]

    if filetypes.has_key(ext[1:]):
        ftype = filetypes[ext[1:]]['ftype']
    else:
        ftype = "File"

    # Start of query
    query = "MATCH (u:User {email: '%s'}) " % email
    
    # If directly in root
    if folder_path == "/":
        query += "MERGE (f:File {name: '%s', path: '%s', type: '%s', ext: '%s'})-[:SHARED_BY]->(u)" % (filename, path, ftype, ext)
    else:
        temp_path = ""

        for idx, folder in enumerate(folder_list):
            temp_path += '/' + folder

            if idx == 0:
                query += "MERGE (f%d:Folder {name: '%s', path: '%s'})-[:SHARED_BY]->(u) " % (idx, folder, temp_path)
            else:
                query += "MERGE (f%d:Folder {name: '%s', path: '%s'})<-[:CONTAINS]-(f%d) " % (idx, folder, temp_path, idx-1)
             
        if not folder_path+"/" == path:
            query += "MERGE (f:File {name: '%s', path: '%s', type: '%s', size: '%s', ext: '%s'})<-[:CONTAINS]-(f%d)" % (filename, path, ftype, size, ext, idx)

    # Run session
    with driver.session() as session:
        res = session.run(query)
        session.sync()



def collect_files(directory):
    ''' Collects a list of files, traversing subfolders, from a root directory
        args:
            string directory = Root share-folder
    '''

    file_list = []

    for root, dirs, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            size = os.path.getsize(path)
            entry = {"path": path, "size": size}
            file_list.append(entry)
    
    return file_list


def run_test_db(driver, filetypes):
    # Path's to test directories
    user_dirs = {}
    user_dirs['helge'] = '/home/keem/Documents/INF-3701/assignment-2/testenv/Helge/'
    user_dirs['jon'] = '/home/keem/Documents/INF-3701/assignment-2/testenv/Jon/'
    user_dirs['kim'] = '/home//keem/Documents/INF-3701/assignment-2/testenv/Kim/'

    # Add users
    add_user(driver, "helge@mail.com", "Helge Hoff", "Online", "33.32.5.67:7979", ["jon@mail.com"])
    add_user(driver, "jon@mail.com", "Jon Foss Mikalsen", "Online", "56.423.1.47:7979", [])
    add_user(driver, "kim@mail.com", "Kim Andreassen", "Offline", "63.22.55.627:7979", ["helge@mail.com", "jon@mail.com"])
    
    # Add messages
    add_message(driver, "jon@mail.com", "02-04-05-11:24:53", "First message")
    add_message(driver, "jon@mail.com", "03-04-05-13:34:23", "One more message")
    add_message(driver, "kim@mail.com", "07-02-12-19:54:13", "Hello, I am Kim")

    # Collect files from root root directories of users
    for key, val in user_dirs.iteritems():
        files = collect_files(val)

        # Add the files
        for f in files:
            temp_path = '/' + os.path.relpath(f['path'], start=val)
            add_full_path(driver, "%s@mail.com" % key, temp_path, f['size'], filetypes)


if __name__ == '__main__':  
    # Init Neo4J
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "kimsekosen"))

    # Init filtypes from yml:
    with open('filetypes.yml', 'r') as f:
        filetypes = yaml.load(f)

    # Run sample test
    run_test_db(driver, filetypes)
