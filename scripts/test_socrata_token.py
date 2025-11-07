#!/usr/bin/env python3
"""
Quick script to test Socrata API token locally
Usage: python scripts/test_socrata_token.py YOUR_TOKEN_HERE
"""

import sys
import requests

def test_token(token: str):
    """Test if a Socrata API token is valid"""
    base_url = "https://data.cityofnewyork.us"
    dataset_id = "hg8x-zxpr"
    
    test_url = f"{base_url}/resource/{dataset_id}.json"
    headers = {"X-App-Token": token}
    params = {"$limit": 1}
    
    print(f"Testing token: {token[:10]}...")
    print(f"URL: {test_url}")
    print()
    
    try:
        resp = requests.get(test_url, headers=headers, params=params, timeout=10)
        
        print(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                print("✅ Token is VALID!")
                print(f"   Successfully fetched {len(data)} test record(s)")
                return True
            else:
                print("⚠️  Unexpected response format")
                print(f"   Response: {data}")
                return False
        elif resp.status_code == 401:
            print("❌ Token is INVALID or EXPIRED (401 Unauthorized)")
            print("   Please get a new token from: https://data.cityofnewyork.us/profile/app_tokens")
            return False
        elif resp.status_code == 403:
            print("❌ Token is FORBIDDEN (403 Forbidden)")
            return False
        else:
            print(f"❌ Error: Status {resp.status_code}")
            print(f"   Response: {resp.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_socrata_token.py YOUR_TOKEN_HERE")
        print()
        print("To get a token:")
        print("1. Visit: https://data.cityofnewyork.us/profile/app_tokens")
        print("2. Login and create/get your token")
        print("3. Run this script with your token")
        sys.exit(1)
    
    token = sys.argv[1]
    success = test_token(token)
    sys.exit(0 if success else 1)

