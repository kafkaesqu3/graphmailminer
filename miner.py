import sys
import requests
import json
from requests.utils import requote_uri
import sqlite3
import dateutil.parser
from datetime import datetime, timedelta
from os.path import exists, abspath

import signal
import sys

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    print("Closing db connection")
    global conn
    conn.close()
    sys.exit(0)

baseurl = "https://graph.microsoft.com/v1.0/me/"
with open ("bearer", "r") as tokenfile:
  bearer = tokenfile.readlines()

# False if we dont want to look attachments
attachmentDetails = True


conn, c = "", "" #globals
def checkAuth():
  results = makeRequest(baseurl)
  if not results: 
    return False
  json_results = json.loads(results.text)
  user = json_results["mail"]
  print("Authentication successful: {}".format(user))
  return user

def emlDbInit(user):
  sqlite_file = 'emails.sqlite'
  

  abs_path = abspath(sqlite_file)
  if exists(abs_path): 
      print("Database exists")
      conn = sqlite3.connect(sqlite_file)
      c = conn.cursor()
      return (conn, c)
  else: 
      #if not, create the file and return conn string
      print("Database does not exists")
      conn = sqlite3.connect(sqlite_file)
      c = conn.cursor()
  

  return (conn, c)

def dbInit(user): 
  sqlite_file = "{}.sqlite".format(user)

  #check if file exists in pwd
  #if exists, open it and return conn string
  abs_path = abspath(sqlite_file)
  if exists(abs_path): 
      print("Database exists")
      conn = sqlite3.connect(sqlite_file)
      c = conn.cursor()
      return (conn, c)
  else: 
      #if not, create the file and return conn string
      print("Database does not exists")
      conn = sqlite3.connect(sqlite_file)
      c = conn.cursor()

      table_name = "inbox"
      # Creating a new SQLite table with 1 column
      c.execute('CREATE TABLE {tn} ({nf} {ft} NOT NULL PRIMARY KEY)'.format(tn=table_name, nf='id', ft='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='receivedDateTime', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='sender', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='subject', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='toRecipients', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='ccRecipients', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='body_html', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='importance', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='isRead', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='hasAttachments', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='attachments', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='parentFolderId', ct='TEXT'))

      table_name = 'emails'
      c.execute('CREATE TABLE {tn} ({nf} {ft} NOT NULL PRIMARY KEY)'.format(tn=table_name, nf='id', ft='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='email', ct='TEXT'))

      table_name = 'attachments'
      c.execute('CREATE TABLE {tn} ({nf} {ft} NOT NULL PRIMARY KEY)'.format(tn=table_name, nf='attachment_id', ft='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='email_id', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='filename', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='contentType', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='size', ct='TEXT'))
      c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name, cn='contents', ct='TEXT'))


      return (conn, c)

def makeRequest(URL):
  payload = {}
  headers = {
    'Authorization': 'Bearer {}'.format(bearer),
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0',
    'Accept': 'application/json'
  }
  URL = requote_uri(URL)
  response = requests.request("GET", URL, headers=headers, data = payload)
  if response.status_code != 200: 
    print("ERROR STATUS: {}".format(response.status_code))
    print("{}".format(response.text))
    return False
  return response #.text.encode('utf8')

 
def getChildMailFolders(folder_id, layer=1): 
  endpoint = baseurl + f"/mailfolders/{folder_id}/childFolders"
  child_results = makeRequest(endpoint)
  if not child_results: 
    return False
  child_json_results = json.loads(child_results.text)
  for page in child_json_results["value"]:
    # Number {i}: {num:{field_size}.2f}
    print("-"*layer + "{name:<{l}} {totalItemCount:<10} {unreadItemCount:<10} {id:<50}".format(l=(30-layer), name=page["displayName"], totalItemCount=page["totalItemCount"], unreadItemCount=page["unreadItemCount"], id=page["id"]))
    if page["childFolderCount"] > 0:
      getChildMailFolders(page["id"], layer+1)


def listMailFolders():
  endpoint = baseurl + "/mailfolders/"
  results = makeRequest(endpoint)
  if not results: 
    return False
  json_results = json.loads(results.text)
  print("{:<30} {:<10} {:<10} {:<50}".format('Name','Msg #','Unread #','id'))
  next_link = True # initialize
  while (next_link): 
    if "@odata.nextLink" in json_results.keys(): 
      next_link = json_results["@odata.nextLink"]
    else: 
      next_link = False
    for page in json_results["value"]: 
      print("{:<30} {:<10} {:<10} {:<50}".format(page["displayName"], page["totalItemCount"], page["unreadItemCount"], page["id"]))
      if page["childFolderCount"] > 0: 
        pass
        getChildMailFolders(page["id"])
    if not next_link: 
      break
    results = makeRequest(next_link)
    if not results: 
      return False
    json_results = json.loads(results.text)

# gives us the full MIME RFC 822 message, given the ID
def getFullEmail(id): 
  endpoint = baseurl + f"messages/{id}/$value"
  response = makeRequest(endpoint)
  if not response: 
    return False
  return response

def getMsgAttachmentMetadata(msgId):
  # https://docs.microsoft.com/en-us/graph/api/message-list-attachments?view=graph-rest-1.0&tabs=http
  endpoint = baseurl + "messages/{}/attachments".format(msgId)
  response = makeRequest(endpoint)
  if not response: 
    return False
  return response

# if you get DatabaseError: database disk image is malformed error here, make sure you arent running this on VMware share
def saveMessageDetails(email): 
  receivedDateTime = dateutil.parser.parse(email["receivedDateTime"])
  subject = email["subject"]
  sender_name = email["from"]["emailAddress"]["name"]
  sender_email = email["from"]["emailAddress"]["address"]
  # flatten list objects
  toRecipients = '; '.join(["{} <{}>".format(address["emailAddress"]["name"], address["emailAddress"]["address"]) for address in email["toRecipients"]])
  ccRecipients = '; '.join(["{} <{}>".format(address["emailAddress"]["name"], address["emailAddress"]["address"]) for address in email["ccRecipients"]])
  body = email["body"]["content"]
  hasAttachments = email["hasAttachments"]
  importance = email["importance"]
  isRead = email["isRead"]
  email_id = email["id"]
  parentFolderId = email["parentFolderId"]

  try:
    query = """INSERT INTO inbox (id,receivedDateTime,sender,subject,toRecipients,ccRecipients,body_html,importance,isRead,hasAttachments,attachments,parentFolderId) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);"""
    c.execute(query, (email_id, receivedDateTime.isoformat(), f"{sender_name} <{sender_email}>", subject, toRecipients, ccRecipients, body, importance, isRead, hasAttachments, None, parentFolderId))
    conn.commit()
  except sqlite3.IntegrityError:
    print('ERROR mailbox: ID already exists in PRIMARY KEY column {}'.format(email_id))
  except KeyboardInterrupt: 
    print("Closing db connection")
    conn.close()


  if email["hasAttachments"] == True and attachmentDetails == True: 
    attachments = getMsgAttachmentMetadata(email["id"])
    # TODO if there's more than 25 attachments....sorry
    attachments = (json.loads(attachments.text))["value"]
    for attachment in attachments: 
      attach_id = attachment["id"]
      #endpoint = "/mailFolders/{email_id}/messages/{attach_id}/attachments"
      #results = makeRequest(endpoint)
      if attachment.has_key("contentBytes"): 
        attachmentContents = attachment["contentBytes"]
      else: 
        attachmentContents = "ERROR: Attachment content not retrieved by miner (not available in server response)"
      try:
          query = """INSERT INTO attachments (attachment_id,email_id,filename,contentType,size,contents) VALUES(?,?,?,?,?,?);"""
          c.execute(query, (attach_id, email["id"], attachment["name"], attachment["contentType"], attachment["size"], attachmentContents))
          conn.commit()
      except sqlite3.IntegrityError:
          print('ERROR attachments: ID already exists in PRIMARY KEY column {}'.format(attachment["id"]))

# fetch emails from a folder, optionally supply how many days of emails you want to grab (0=grab em all)
# does not print emails to stdout; saves them to a sqlite database for analysis
# returns # of emails we saved
def listMailbox(folder=None, searchfilter=None): 
  count = 0
  if folder == None: # all mailboxes
    endpoint = baseurl + "messages"
  else: 
    endpoint = baseurl + f"mailfolders/{folder}/messages"
  endpoint = endpoint + "?$select=sender,subject,from,toRecipients,ccRecipients,receivedDateTime,body,isRead,hasAttachments,categories,importance,id,parentFolderId&$top=25&$orderby=receivedDateTime%20DESC"
  if searchfilter: 
    endpoint = endpoint + f"&$filter={searchfilter}"
  results = makeRequest(endpoint)
  if not results: 
    return False
  json_results = json.loads(results.text)
  next_link = True # initialize
  while (next_link): 
    if "@odata.nextLink" in json_results.keys(): 
      next_link = json_results["@odata.nextLink"]
    else: 
      next_link = False
    for email in json_results["value"]: 
      count = count + 1
      saveMessageDetails(email)
    if not next_link: 
      break
    results = makeRequest(next_link)
    if not results: 
      return False
    json_results = json.loads(results.text)
  return count

# get last # of messages receives by the yser
def getLastNMessages(): 
  print("not implemented")
  pass

# get all the messages from the last X days
def getRecentMessages(days_to_pull): 
  d = datetime.today()
  d = datetime.today() - timedelta(days=days_to_pull)
  d = d.date.isoformat()

  searchfilter = "filter=ReceivedDateTime ge {d}}"
  receivedCount = listMailbox(searchfilter=searchfilter)


def getMessagesBetweenDates(date1, date2):
  # convert date strings to ISO8601 format
  try: 
    date1 = datetime.strptime(date1, '%Y-%m-%d')
    date1 = date1.date().isoformat()
    date2 = datetime.strptime(date2, '%Y-%m-%d')
    date2 = date2.date().isoformat()
  except ValueError: 
    print("Incorrect timestamps. Expected YYYY-MM-DD")
    return 0
  
  if date1 < date2: 
    searchfilter = f"ReceivedDateTime ge {date1} and receivedDateTime lt {date2}"
  else: 
    searchfilter = f"ReceivedDateTime ge {date2} and receivedDateTime lt {date1}"
  receivedCount = listMailbox(searchfilter=searchfilter)
  return receivedCount

def getMessagesWithAttachments(): 
  #  https://graph.microsoft.com/v1.0/me/messages?filter=HasAttachments eq true
  pass

# messages received to someone/some domain
def getMessagesFromSender(sender_email): 
  # me/messages?$search="from:admin@allstate.com"
  # me/messages?$search="from:@allstate.com"
  pass

# messages sent to someone/some domain
def getMessagesToRecipient(recipient_email): 
  # me/messages?$search="to:admin@allstate.com"
  # me/messages?$search="to:@allstate.com"
  pass

# search by subject/body
def searchMail(searchTerm): 
  # using search folders
  # https://docs.microsoft.com/en-us/graph/api/resources/mailsearchfolder?view=graph-rest-1.0

  # using OData query params
  # https://docs.microsoft.com/en-us/graph/query-parameters#using-search-on-message-collections

  # https://docs.microsoft.com/en-us/graph/query-parameters#search-parameter

  # save results
  # saveMessageDetails(email)
  pass

def main():
  user = checkAuth()
  if not user: 
    print("Authentication FAILED")
    sys.exit(1)
  
  global conn
  global c
  conn, c = dbInit(user)

  """ try: 
    conn, c = dbInit(user)
  except SystemExit as e:
    conn.close() """

  #listMailFolders()  
  date1 = "2020-07-27"
  date2 = "2019-01-01"
  #listMailFolders()
  messageCount = getMessagesBetweenDates(date1, date2)
  #print("Got {} messages".format(messageCount))

main()
