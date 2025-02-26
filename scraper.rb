require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'
require 'cgi'

# Set up a logger to log the scraped data
logger = Logger.new(STDOUT)

# Define the URL of the page
url = "https://www.ccc.tas.gov.au/development/advertised-plans/"

# Step 1: Fetch the page content
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 2: Parse the page content using Nokogiri
page = Nokogiri::HTML(page_html)

# Step 2: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create a table specific to Clarence Council if it doesn't exist
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS clarence (
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

# Loop through document list entries
page.search('.doc-list a').each do |a|
  unless a.at('img')  # Skip if it's an image link
    name = a.inner_text.strip
    s = name.split(' - ').map(&:strip)

    # Ensure it follows the expected format
    if s.count != 4
      logger.warn("Unexpected form of PDF name. Skipping: #{name}")
      next
    end

    council_reference = s[0]
    address = "#{s[1]}, TAS"
    description = s[2]

    # Extract on_notice_to as a valid date
    begin
      on_notice_to = Date.parse(s[3]).to_s
    rescue ArgumentError
      on_notice_to = "Invalid Date"
    end

    application_url = (page.uri + a["href"]).to_s

    # Log the extracted data
    logger.info("Council Reference: #{council_reference}")
    logger.info("Description: #{description}")
    logger.info("Address: #{address}")
    logger.info("Closing Date: #{on_notice_to}")
    logger.info("View Details Link: #{application_url}")
    logger.info("-----------------------------------")

    # Ensure the entry does not already exist before inserting
    existing_entry = db.execute("SELECT * FROM clarence WHERE council_reference = ?", council_reference)

    if existing_entry.empty?
      db.execute("INSERT INTO clarence 
        (council_reference, description, address, date_received, on_notice_to, date_scraped, application_url) 
        VALUES (?, ?, ?, ?, ?, ?, ?)",
        [council_reference, description, address, nil, on_notice_to, date_scraped, application_url])

      logger.info("Data for #{council_reference} saved to database.")
    else
      logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
    end
  end
end

logger.info("Data has been successfully inserted into the database.")
