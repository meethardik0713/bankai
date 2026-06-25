"""
Run this script to generate all PWA icon sizes from your logo.
Put your logo as 'logo.png' (512x512 or bigger) in the same folder.

Usage: python generate_icons.py
"""
from PIL import Image
import os

SIZES = [72, 96, 128, 144, 152, 192, 384, 512]
OUTPUT_DIR = 'static/icons'

os.makedirs(OUTPUT_DIR, exist_ok=True)

logo = Image.open('logo.png')  # <-- apna logo yahan rakh

for size in SIZES:
    resized = logo.resize((size, size), Image.LANCZOS)
    resized.save(os.path.join(OUTPUT_DIR, f'icon-{size}.png'))
    print(f'Created icon-{size}.png')

print('Done! All icons generated.')
