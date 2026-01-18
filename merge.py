import shutil
import os

def create_final_database():
    old_folder = "parsed_content_markdowns"
    retry_folder = "parsed_content_markdowns2"
    final_folder = "FINAL_DATABASE"

    if not os.path.exists(final_folder):
        os.makedirs(final_folder)
        print(f"📂 Created folder: {final_folder}")

    print("🚚 Copying initial data...")
    if os.path.exists(old_folder):
        for root, dirs, files in os.walk(old_folder):
            for file in files:
                if file == "_error_summary.csv": continue
                
                src_path = os.path.join(root, file)
                rel_path = os.path.relpath(src_path, old_folder)
                dst_path = os.path.join(final_folder, rel_path)
                
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy2(src_path, dst_path)

    print("🛠️ Patching with fixed files...")
    count = 0
    if os.path.exists(retry_folder):
        for root, dirs, files in os.walk(retry_folder):
            for file in files:
                if file.endswith(".csv"): continue
                
                src_path = os.path.join(root, file)
                rel_path = os.path.relpath(src_path, retry_folder)
                dst_path = os.path.join(final_folder, rel_path)
                
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy2(src_path, dst_path)
                count += 1

    print("-" * 30)
    print(f"✅ DONE! Your integrated database is ready in: /{final_folder}")
    print(f"✨ Total fixed files integrated: {count}")

if __name__ == "__main__":
    create_final_database()