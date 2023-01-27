from utils.DatabaseInfo import DatabaseInfo
from utils.ColorDescriptor import ColorDescriptor

import os
import pymysql
import glob
import cv2

print("RHYA.Network Anim-Image-Analyze-API")
print("Image Feature Value Extraction and Storage API (RHYA.Network only)")
print("")
print("Copyright (c) 2023 RHYA.Network. All rights reserved.")
print("")
print("A file scan is performed against the path in the result array.")
print("Please specify folder path only for result array.")
print("We hereby notify you that the data is not commercially available.")
print("")

# Root paths
# ======================================
result = [
    ["C:\\Users\\ji055\\Desktop\\123", 1]
]
# ======================================

cd = ColorDescriptor((8, 12, 3))

conn = pymysql.connect(host=DatabaseInfo.host,
                       user=DatabaseInfo.username,
                       password=DatabaseInfo.password,
                       db=DatabaseInfo.database,
                       charset='utf8')

remover_result = []
remover_cursor = conn.cursor()
remover_cursor.execute("SELECT analyze_id, analyze_path FROM anim_image_analyze")

while True:
    row = remover_cursor.fetchone()

    if row is None:
        break

    if not os.path.exists(row[1]):
        remover_result.append([row[0], row[1]])

remover_cursor.close()

remover_task = conn.cursor()

for remove_info in remover_result:
    remove_row_id = remove_info[0]
    remove_path = remove_info[1]

    os.remove(remove_path)

    remover_task.execute("DELETE FROM anim_image_analyze WHERE analyze_id = %s" % remove_row_id)

conn.commit()
remover_task.close()

cur = conn.cursor()

for analyze_metadata in result:
    analyze_path = analyze_metadata[0]
    analyze_type = analyze_metadata[1]

    print("[Target Path] %s" % analyze_path)

    for image_path in glob.glob(analyze_path + "/*"):
        print("[File Path] %s" % image_path)

        try:
            image = cv2.imread(image_path)
            features = cd.describe(image)
            features = [str(f) for f in features]

            sql = "INSERT INTO anim_image_analyze(analyze_value, analyze_path, analyze_target_type) VALUES ('%s', '%s', %d)" % (str(features).replace("'", "\""), str(image_path).replace("\\", "\\\\"), analyze_type)
            cur.execute(sql)
        except Exception as e:
            print("[Error] %s" % e)

conn.commit()
cur.close()
conn.close()
