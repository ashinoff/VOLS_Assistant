import re
import requests

def extract_folder_id(drive_url):
    match = re.search(r'/folders/([a-zA-Z0-9_-]+)', drive_url)
    if not match:
        raise ValueError("Некорректная ссылка на Google Drive папку")
    return match.group(1)

def get_public_folder_files(folder_url):
    folder_id = extract_folder_id(folder_url)
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#grid"
    resp = requests.get(url, timeout=15)
    if resp.status_code != 200:
        return []
    files = []
    for match in re.finditer(r'data-id="([^"]+)"[\s\S]+?data-type="[^"]+"[\s\S]+?aria-label="([^"]+)"', resp.text):
        file_id, file_name = match.groups()
        file_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
        files.append((file_name, file_url))
    return files
