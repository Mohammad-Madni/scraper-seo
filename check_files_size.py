import os
import csv

def check_file_sizes(directory, min_size_kb=5):
    # تبدیل کیلوبایت به بایت
    min_size_bytes = min_size_kb * 1024
    results = []
    
    if not os.path.exists(directory):
        print(f"❌ Error: Directory '{directory}' not found!")
        return

    print(f"🔍 Scanning files in '{directory}' for sizes below {min_size_kb}KB...")
    
    # لیست کردن تمام فایل‌های داخل پوشه
    files = [f for f in os.listdir(directory) if f.endswith('.md')]
    
    low_quality_count = 0
    total_files_scanned = 0
    
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.md'):  # Only .md files
                filepath = os.path.join(root, filename)
                file_size = os.path.getsize(filepath)
                total_files_scanned += 1
                
                if file_size < min_size_bytes:
                    size_kb = round(file_size / 1024, 2)
                    results.append({
                        'file_name': filename,
                        'full_path': filepath,  # ✅ Added full path for clarity
                        'size_kb': size_kb,
                        'status': '⚠️ LOW_CONTENT'
                    })
                    low_quality_count += 1
                    print(f"⚠️ Warning: {filepath} is only {size_kb}KB")
    # ذخیره نتایج در یک فایل CSV
    output_file = 'low_quality_content_report.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        # Added 'full_path' to fieldnames to match the data structure
        writer = csv.DictWriter(f, fieldnames=['file_name', 'full_path', 'size_kb', 'status'])
        writer.writeheader()
        writer.writerows(results)

    print("\n" + "="*30)
    print(f"✅ Scan Complete!")
    print(f"📊 Total Files Scanned: {total_files_scanned}")
    print(f"🚨 Low Quality Files Found: {low_quality_count}")
    print(f"📝 Report saved to: {output_file}")
    print("="*30)

if __name__ == "__main__":
    # مسیر پوشه‌ای که فایل‌های مارک‌داون در آن هستند
    target_directory = 'parsed_content_markdowns'
    check_file_sizes(target_directory)
