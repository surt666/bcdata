#!/usr/bin/env python3
import re

# Read the original SQL file
with open('sample_inserts.sql', 'r', encoding='utf-8') as f:
    content = f.read()

# Extract all INSERT statements
insert_pattern = r'INSERT INTO billingdata\.meters \((.*?)\) VALUES \((.*?)\);'
matches = re.findall(insert_pattern, content, re.DOTALL)

if not matches:
    print("No INSERT statements found!")
    exit(1)

# Get the column list from the first INSERT (they're all the same)
columns = matches[0][0].strip()

# Collect all VALUES clauses
values_list = []
for match in matches:
    values = match[1].strip()
    values_list.append(f"  ({values})")

# Create the combined INSERT statement
with open('sample_inserts_athena.sql', 'w', encoding='utf-8') as f:
    f.write("-- Combined INSERT statement for Athena console\n")
    f.write("-- All 120 rows inserted in a single statement\n\n")
    f.write(f"INSERT INTO billingdata.meters (\n  {columns}\n) VALUES\n")
    f.write(",\n".join(values_list))
    f.write(";\n")

print(f"Reformatted SQL file created: sample_inserts_athena.sql")
print(f"Combined {len(values_list)} INSERT statements into a single multi-row INSERT")
