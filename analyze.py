from utils.DatabaseInfo import DatabaseInfo
from utils.ColorDescriptor import ColorDescriptor

import os
import pymysql
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

cd = ColorDescriptor((8, 12, 3))

conn = pymysql.connect(host=DatabaseInfo.host,
                       user=DatabaseInfo.username,
                       password=DatabaseInfo.password,
                       db=DatabaseInfo.database,
                       charset='utf8')


# Root paths
# ======================================
target = []

metadata_cursor = conn.cursor()
metadata_cursor.execute("SELECT * FROM anim_image_analyze_metadata;")

while True:
    row = metadata_cursor.fetchone()

    if row is None:
        break

    target.append([row[2], row[0]])

metadata_cursor.close()
# ======================================

database_data = []
remover_result = []
remover_cursor = conn.cursor()
remover_cursor.execute("SELECT analyze_id, analyze_file_name, analyze_target_type, target_save_path FROM anim_image_analyze odata LEFT JOIN anim_image_analyze_metadata metadata ON odata.analyze_target_type = metadata.target_type_id;")

while True:
    row = remover_cursor.fetchone()

    if row is None:
        break

    path = os.path.join(row[3], row[1])

    if not os.path.exists(path):
        remover_result.append([row[0], path])
    else:
        database_data.append(path)

remover_cursor.close()

remover_task = conn.cursor()

for remove_info in remover_result:
    try:
        remove_row_id = remove_info[0]
        remove_path = remove_info[1]

        remover_task.execute("DELETE FROM anim_image_analyze WHERE analyze_id = '%s'" % remove_row_id)

        os.remove(remove_path)
    except Exception as e:
        print("[Error] %s" % e)

conn.commit()
remover_task.close()

cur = conn.cursor()

for analyze_metadata in target:
    analyze_path = analyze_metadata[0]
    analyze_type = analyze_metadata[1]

    print("[Target Path] %s" % analyze_path)

    now_index = 0
    max_index = len(os.listdir(analyze_path))

    for image_file_name in os.listdir(analyze_path):

        now_index = now_index + 1

        try:
            image_path = os.path.join(analyze_path, image_file_name)

            if image_path in database_data:
                print("[File Path] %s --> Skip! (%d / %d)" % (image_path, now_index, max_index))
                continue
            else:
                print("[File Path] %s (%d / %d)" % (image_path, now_index, max_index))

            image = cv2.imread(image_path)
            features = cd.describe(image)
            features = [str(f) for f in features]

            sql = "INSERT INTO anim_image_analyze(analyze_value, analyze_file_name, analyze_target_type) VALUES ('%s', '%s', %d)" % (str(features).replace("'", "\""), str(image_file_name), analyze_type)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print("[Error] %s" % e)

cur.close()
conn.close()
