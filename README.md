# Introduction

This application watches a set of directories and processes the files inside them. It extracts the content of the file and indexes
it in **Elasticsearch**. The file is being stored under an Apache HTTP server so that it can be downloaded while user is viewing
the content of the file. As a UI for the user it is recommended to use **Kibana**.

# Prerequisites 

- Elasticsearch 6.3
- Kibana 6.3
- Apache HTTP server

# How to run the application

1. At first start the Elasticsearch service as the application uses a TransportClient which considers that there's a running Elasticsearch cluster

2. Configure Apache HTTP server
   - By default it uses the following IP: 127.0.1.1 
   - Set the **DocumentRoot** in the */etc/apache2/sites-enabled/000-default* file to the directory where the application has to be run. In my case it is set to */home/david/IdeaProjects/emirates*
   - Set  **AllowEncodedSlashes On** in the */etc/apache2/sites-enabled/000-default*  file

3. Application gets as an argument the directory path which it is going to monitor. The directory has to contain the following folders:
   - archive
   - images
   - txt
   - emails
  
  After doing the previous steps correctly you can test the application by putting corresponding files in the directories. e.g.
  If you put test.jpg into images directory the metadata of that image will be extracted and indexed as a document under the "images" index,
  the _id of the document will serve the file path (relative to the current working directory). This will later allow us to download the file from Kibana UI if needed.
 
 # Configure the UI
 
 Configure Kibana for viewing and downloading files:
 - Create an index pattern for each index (archive, images, txt, emails)
 - Change the type of an _id field to URL and put as a URL template the following pattern: http://127.0.1.1/{{value}}

 If you successfully do the steps you'll be able to download the file by clicking on the value of the _id field from Kibana

