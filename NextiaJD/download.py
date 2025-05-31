#!/usr/bin/env python3
import os
import sys
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


def download_all_files(base_url, dest_dir):
    """Fetches the directory listing at base_url, finds all files, and downloads them into dest_dir."""
    print(f"🔽 Download target directory: {dest_dir}\n")

    resp = requests.get(base_url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')
    links = soup.find_all('a')

    for link in links:
        href = link.get('href')
        # skip parent directory links, subdirectories, and query/sort links
        if not href or href in ('../',) or href.endswith('/') or '?' in href:
            continue

        file_url = urljoin(base_url, href)
        out_path = os.path.join(dest_dir, href)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        # Check remote file size via HEAD
        try:
            head = requests.head(file_url, allow_redirects=True)
            head.raise_for_status()
            remote_size = int(head.headers.get('Content-Length', 0))
        except Exception:
            remote_size = None

        # Decide whether to download
        if os.path.exists(out_path) and remote_size is not None:
            local_size = os.path.getsize(out_path)
            if local_size == remote_size:
                print(f"🔄 Skipping (already exists): {out_path} ({local_size} bytes)")
                continue
            else:
                print(f"⚠️  Size mismatch, re-downloading: {out_path} (local {local_size} vs remote {remote_size})")
        else:
            print(f"⬇️  Downloading: {file_url}\n    → {out_path}")

        # Stream download
        r = requests.get(file_url, stream=True)
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"✅ Saved to: {out_path}\n")


def main():
    # Hardcoded source URL
    BASE_URL = 'https://event.cwi.nl/da/NextiaJD/'

    # Destination directory: 'temp' folder relative to this script
    script_dir = os.path.abspath(os.path.dirname(__file__))
    dest_dir = os.path.join(script_dir, 'temp')

    os.makedirs(dest_dir, exist_ok=True)

    try:
        download_all_files(BASE_URL, dest_dir)
        print(f"\n🎉 All files processed. Final files are in:\n    {dest_dir}")
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
