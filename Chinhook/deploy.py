import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. تحميل المتغيرات من ملف .env
load_dotenv()

# 2. الحصول على رابط قاعدة البيانات من البيئة المحيطة
DB_URL = os.getenv("DB_URL")

file_list = [
    "Album.csv", "Artist.csv", "Customer.csv", "Employee.csv", 
    "Genre.csv", "Invoice.csv", "InvoiceLine.csv", "MediaType.csv", 
    "Playlist.csv", "PlaylistTrack.csv", "Track.csv"
]

# التأكد من وجود الرابط قبل محاولة الاتصال
if not DB_URL:
    print("Error: DB_URL not found in .env file!")
else:
    engine = create_engine(DB_URL)

    for file in file_list:
        try:
            # قراءة الملف
            temp_df = pd.read_csv(file)
            
            # استخراج اسم الجدول (مثلاً Album)
            table_name = file.split('.')[0]
            
            # رفع البيانات إلى PostgreSQL
            # ملاحظة: سيتم إنشاء الجداول بأسماء حساسة لحالة الأحرف (Case-Sensitive) بسبب pandas
            temp_df.to_sql(table_name, engine, if_exists="replace", index=False)
            
            print(f"✅ Successfully uploaded {file} to table '{table_name}'")
            
        except Exception as e:
            print(f"❌ Error uploading {file}: {e}")

    print("\n--- Verifying table 'Artist' ---")
    with engine.connect() as conn:
        try:
            # نستخدم الكوتيشن المزدوجة لأن pandas ترفع الجداول بأسماء Case-Sensitive
            result = conn.execute(text('SELECT * FROM "Artist" LIMIT 5;'))
            for row in result:
                print(row)
        except Exception as e:
            print(f"⚠️ Could not verify: {e}")