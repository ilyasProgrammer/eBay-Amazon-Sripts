import csv
# with open('/home/ra/Downloads/b.csv', 'rb') as inp, open('inact.csv', 'wb') as out:
#     writer = csv.writer(out)
#     for i, row in enumerate(csv.reader(inp, dialect="excel-tab")):
#         if "make auto parts manufacturing" in row[0].lower() or "mapm" in row[0].lower():
#             writer.writerow(row)
#             print i

# with open('/home/ra/Downloads/inact.csv', 'rb') as inp, open('inact_no_for.csv', 'wb') as out:
#     writer = csv.writer(out, delimiter="\t")
#     for i, row in enumerate(csv.reader(inp, delimiter="\t")):
#         if "for " not in row[0].lower():
#             if "mapm" in row[0].lower() or "make auto parts manufacturing" in row[0].lower() :
#                 writer.writerow(row)
#                 print i

with open('/home/ra/Downloads/act.csv', 'rb') as inp, open('act_no_for.csv', 'wb') as out:
    writer = csv.writer(out, delimiter="\t")
    for i, row in enumerate(csv.reader(inp, delimiter="\t")):
        if "mapm" in row[2].lower() or "make auto" in row[2].lower():
        # if "mapm" not in row[2].lower() and "make auto" not in row[2].lower():
            if "for " not in row[2].lower():
                # if 'TOYOTA' in row[2].upper() or 'HYUNDAI' in row[2].upper():
                    writer.writerow(row)
                    print i
