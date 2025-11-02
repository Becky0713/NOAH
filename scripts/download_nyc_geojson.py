"""
Download NYC Census Tracts GeoJSON for rent burden visualization
"""

import os
import requests
import json
from pathlib import Path

# NYC Census Tracts GeoJSON
GEOJSON_URL = "https://data.cityofnewyork.us/resource/37yn-as6i.geojson"
OUTPUT_DIR = Path(__file__).parent.parent / "frontend" / "data"
OUTPUT_FILE = OUTPUT_DIR / "nyc_tracts.geojson"

def download_geojson():
    """Download NYC Census Tracts GeoJSON"""
    print("📥 Downloading NYC Census Tracts GeoJSON...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        response = requests.get(GEOJSON_URL, timeout=60)
        response.raise_for_status()
        
        # Save to file
        with open(OUTPUT_FILE, 'wb') as f:
            f.write(response.content)
        
        file_size = len(response.content) / 1024 / 1024
        print(f"✅ Successfully downloaded {OUTPUT_FILE}")
        print(f"📦 File size: {file_size:.2f} MB")
        
        # Basic validation
        import json
        with open(OUTPUT_FILE, 'r') as f:
            data = json.load(f)
        
        print(f"📍 Features: {len(data.get('features', []))}")
        print(f"🗺️  CRS: {data.get('crs', {}).get('properties', {}).get('name', 'Unknown')}")
        
    except requests.RequestException as e:
        print(f"❌ Download failed: {e}")
        print("\n📥 Please manually download:")
        print("   1. Visit: https://data.cityofnewyork.us/City-Government/2010-Census-Tracts/37yn-as6i")
        print("   2. Click 'Export' → 'GeoJSON'")
        print("   3. Save to:", OUTPUT_FILE)
        return False
    
    return True

if __name__ == "__main__":
    if download_geojson():
        print("\n🎉 GeoJSON ready! You can now use the rent burden visualization.")
        print(f"📂 File location: {OUTPUT_FILE}")
    else:
        print("\n⚠️ Please download GeoJSON manually (see instructions above)")

