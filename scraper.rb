require 'scraperwiki'
require 'mechanize'

require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'

a = Mechanize.new

# Set up a logger to log the scraped data
logger = Logger.new(STDOUT)

# Define the URL of the page
url = "https://www.ccc.tas.gov.au/planning-development/planning/advertised-planning-permit-applications/"

# Step 1: Fetch the page content for the main listing
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create table if it doesn't already exist
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS westtamar (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
title_reference = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = Date.today.to_s

logger.info("Start Extraction of Data")

a.get(url) do |page|
  page.search('.doc-list a').each do |a|
    unless a.at('img')
      # Long winded name of PDF
      name = a.inner_text.strip
      s = name.split(' - ').map(&:strip)
      # Skip over links that we don't know how to handle
      if s.count != 4
        puts "Unexpected form of PDF name. So, skipping: #{name}"
        next
      end

      council_reference = s[0]
      address = s[1] + ", TAS"
      description = s[2]
      on_notice_to = Date.parse(s[3]).to_s
      application_url = (page.uri + a["href"]).to_s

      # Log the extracted data
      logger.info("Council Reference: #{council_reference}")
      logger.info("Description: #{description}")
      logger.info("Address: #{address}")
      logger.info("Closing Date: #{on_notice_to}")
      logger.info("View Details Link: #{application_url}")
      logger.info("-----------------------------------")
    
      # Step 4: Ensure the entry does not already exist before inserting
      existing_entry = db.execute("SELECT * FROM westtamar WHERE council_reference = ?", council_reference)
    
      if existing_entry.empty?  # Only insert if the entry doesn't already exist
        # Save data to the database
        db.execute("INSERT INTO westtamar 
          (council_reference, description, address, date_received, on_notice_to, date_scraped) 
          VALUES (?, ?, ?, ?, ?, ?)",
          [council_reference, description, address, date_received, on_notice_to, date_scraped])
    
        logger.info("Data for #{council_reference} saved to database.")
      else
        logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
      end
      
    end
  end
end
