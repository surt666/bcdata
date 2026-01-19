#!/usr/bin/env python3
import csv
import random

# Set seed for reproducibility
random.seed(42)

# Property ID mapping for each company (each company gets 2 unique property_id values)
property_id_map = {
    3: [1, 2],      # Glostrup Boligselskab
    59: [3, 4],     # Carlsberg Breweries A/S
    67: [5, 6]      # KAB s m b a
}

# Read CSV and generate INSERT statements
with open('meterstatistics.csv', 'r', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)

    with open('sample_inserts.sql', 'w', encoding='utf-8') as sqlfile:
        sqlfile.write("-- INSERT statements for meterstatistics.csv to billingdata.meters table\n")
        sqlfile.write("-- Generated with property_id assignment:\n")
        sqlfile.write("--   Company 3 (Glostrup Boligselskab): property_id randomly from {1, 2}\n")
        sqlfile.write("--   Company 59 (Carlsberg Breweries A/S): property_id randomly from {3, 4}\n")
        sqlfile.write("--   Company 67 (KAB s m b a): property_id randomly from {5, 6}\n")
        sqlfile.write("-- Missing fields (active_management_read, active_garbage_meter_read) set to 0\n")
        sqlfile.write("-- Timestamp set to 2026-01-19 00:00:00\n\n")

        for row in reader:
            company_id = int(row['company'])
            property_id = random.choice(property_id_map[company_id])

            # Escape single quotes in string fields
            company_name = row['company_name'].replace("'", "''")
            building_name = row['building'].replace("'", "''")

            insert_stmt = f"""INSERT INTO billingdata.meters (
  timestamp,
  company_id,
  property_id,
  building_id,
  company_name,
  building_name,
  total,
  actively_remote_read,
  active_manual_read,
  active_calculation_meters,
  inactive_remotely_read,
  inactive_manually_read,
  inactive_calculation_meters,
  unsupported_remotely_read,
  unsupported_manually_read,
  unsupported_calculation_meters,
  active_management_read,
  active_garbage_meter_read
) VALUES (
  timestamp '2026-01-19 00:00:00',
  {company_id},
  {property_id},
  {row['building_id']},
  '{company_name}',
  '{building_name}',
  {row['total']},
  {row['active_remotely_read']},
  {row['active_manual_readings']},
  {row['active_calculation_meters']},
  {row['inactive_remotely_read']},
  {row['inactive_manual_readings']},
  {row['inactive_calculation_meters']},
  {row['unSupported_remotely_read']},
  {row['unSupported_manual_readings']},
  {row['unSupported_calculation_meters']},
  0,
  0
);

"""
            sqlfile.write(insert_stmt)

print("SQL file generated successfully: sample_inserts.sql")
