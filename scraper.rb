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

# Loop through each planning application
page.css('.content-card').each do |card|
  # Extract Council Reference (Application Number)
  council_reference = card.at_css('p:contains("Application Number:")')&.text&.strip&.sub("Application Number:", "")&.strip

  # Extract the full text from <h3 class="content-card__title"> to parse the Address and Description
  full_text = card.at_css('.map-link__text')&.text&.strip

  if full_text
    # Split the text at " – " to separate Address and Description
    parts = full_text.split(" – ")

    # Ensure it has both address and description
    if parts.length >= 2
      address = parts[0].strip  # The first part is the address
      description = parts[1..].join(" – ").split("Advertising period expires").first.strip  # Everything after " – ", excluding expiry notice
    else
      address = "Unknown"
      description = "Unknown"
    end
  else
    address = "Unknown"
    description = "Unknown"
  end

  # Extract Closing Date (on_notice_to)
  on_notice_to_raw = card.at_css('p:contains("Closes:")')&.text&.strip&.sub("Closes:", "")&.strip

  # Convert to YYYY-MM-DD format
  begin
    on_notice_to = Date.parse(on_notice_to_raw).to_s
  rescue ArgumentError
    on_notice_to = "Invalid Date"
  end

  # Extract PDF Link
  document_description = card.at_css('.content-card__button a')&.[]('href')

  # Log the extracted data
  logger.info("Council Reference: #{council_reference}")
  logger.info("Address: #{address}")  # Now correctly extracted
  logger.info("Description: #{description}")  # Now correctly extracted
  logger.info("Closing Date: #{on_notice_to}")
  logger.info("PDF Link: #{document_description}")
  logger.info("-----------------------------------")

  # Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM clarence WHERE council_reference = ?", council_reference)

  if existing_entry.empty?
    db.execute("INSERT INTO clarence 
      (council_reference, description, address, date_received, on_notice_to, date_scraped, document_description) 
      VALUES (?, ?, ?, ?, ?, ?, ?)",
      [council_reference, description, address, nil, on_notice_to, date_scraped, document_description])

    logger.info("Data for #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for document #{council_reference} found. Skipping insertion.")
  end
end


logger.info("Data has been successfully inserted into the database.")
