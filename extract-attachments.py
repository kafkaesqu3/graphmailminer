import sqlite3
import os
import sys
import base64

def dbInit(user): 
  sqlite_file = user

  #check if file exists in pwd
  #if exists, open it and return conn string
  abs_path = os.path.abspath(sqlite_file)
  if os.path.exists(abs_path): 
      print("Database exists")
      conn = sqlite3.connect(sqlite_file)
      c = conn.cursor()
      return (conn, c)
  else: 
      #if not, create the file and return conn string
      print("Database file {} does not exists".format(user))
      return (False, False)

def checkFileExists(filename):
    count = 1
    if os.path.exists(filename): 
        count = count + 1
        filename_noextension = os.path.splitext(filename)[0]
        extension = os.path.splitext(filename)[1]
        filename = filename_noextension + f" ({count})." + extension
        checkFileExists(filename)
    else: 
        return filename

db_file = "target@domain.com.sqlite"
write_folder = db_file.split("@")[0] + "-attachments"
if not os.path.exists(write_folder):
    os.mkdir(write_folder)

conn, c = dbInit(db_file)
if conn == False: 
    sys.exit(1)
query = """SELECT filename, contentType, contents from attachments"""
c.execute(query)
rows = c.fetchall()
for row in rows: 
    filename = row[0]
    unique_filename = checkFileExists(filename)
    binaryContents = base64.b64decode(row[2])
    writepath = os.path.join(write_folder, unique_filename)
    f = open(writepath, 'wb')
    f.write(binaryContents)
